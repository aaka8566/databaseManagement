fn with_live_endpoint<T, F>(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
    action: F,
) -> Result<T, String>
where
    F: FnOnce(&str, u16, &mut WorkerResources) -> Result<T, String>,
{
    let (host, port) = live_endpoint(profile, resources)?;

    action(&host, port, resources)
}

fn ssh_auth(
    session: &mut Session,
    user: &str,
    password: &str,
    private_key_path: &str,
) -> Result<(), String> {
    if !password.is_empty() {
        session
            .userauth_password(user, password)
            .map_err(|e| format!("SSH password auth failed: {e}"))?;
    } else if !private_key_path.trim().is_empty() {
        let key_path = expand_tilde(private_key_path);
        session
            .userauth_pubkey_file(user, None, std::path::Path::new(&key_path), None)
            .map_err(|e| format!("SSH key auth failed: {e}"))?;
    } else {
        session
            .userauth_agent(user)
            .map_err(|e| format!("SSH agent auth failed: {e}"))?;
    }
    if !session.authenticated() {
        return Err("SSH authentication failed".to_owned());
    }
    Ok(())
}

fn new_ssh_session(
    ssh_host: &str,
    ssh_port: u16,
    ssh_user: &str,
    ssh_password: &str,
    ssh_private_key_path: &str,
) -> Result<Session, String> {
    let tcp = TcpStream::connect(format!("{ssh_host}:{ssh_port}"))
        .map_err(|e| format!("SSH TCP connect failed: {e}"))?;
    let mut session = Session::new().map_err(|e| format!("SSH session init failed: {e}"))?;
    session.set_tcp_stream(tcp);
    session
        .handshake()
        .map_err(|e| format!("SSH handshake failed: {e}"))?;
    ssh_auth(&mut session, ssh_user, ssh_password, ssh_private_key_path)?;
    Ok(session)
}

fn live_endpoint(
    profile: &ConnectionProfile,
    resources: &mut WorkerResources,
) -> Result<(String, u16), String> {
    let Some(ssh) = &profile.ssh_tunnel else {
        return Ok((profile.host.clone(), profile.port));
    };

    let cache_key = format!(
        "{}@{}:{}|{}:{}|{}|{}",
        ssh.user,
        ssh.host,
        ssh.port,
        profile.host,
        profile.port,
        ssh.private_key_path,
        if ssh.password.is_empty() {
            "key"
        } else {
            "pwd"
        }
    );

    // Check if we already have a tunnel cached for this key
    if let Some(tunnel) = resources.tunnels.get(&cache_key) {
        return Ok(("127.0.0.1".to_owned(), tunnel.local_port));
    }

    // Validate credentials work before binding the listener
    let ssh_host = ssh.host.clone();
    let ssh_port = ssh.port;
    let ssh_user = ssh.user.clone();
    let ssh_password = ssh.password.clone();
    let ssh_private_key_path = ssh.private_key_path.clone();
    let remote_host = profile.host.clone();
    let remote_port = profile.port;

    // Probe-connect to surface auth errors immediately
    new_ssh_session(
        &ssh_host,
        ssh_port,
        &ssh_user,
        &ssh_password,
        &ssh_private_key_path,
    )?;

    let local_port = find_free_local_port()?;
    let listener = TcpListener::bind(("127.0.0.1", local_port))
        .map_err(|e| format!("Failed to bind local port: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("set_nonblocking failed: {e}"))?;

    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    let handle = thread::spawn(move || {
        let mut connection_threads: Vec<thread::JoinHandle<()>> = Vec::new();

        loop {
            if stop_rx.try_recv().is_ok() {
                break;
            }

            match listener.accept() {
                Ok((local_stream, _)) => {
                    // Each connection gets its own independent SSH session so there
                    // is zero shared mutable state — this is what eliminates the
                    // "Packets out of sync" codec error.
                    let ssh_host2 = ssh_host.clone();
                    let ssh_user2 = ssh_user.clone();
                    let ssh_password2 = ssh_password.clone();
                    let ssh_key2 = ssh_private_key_path.clone();
                    let remote_host2 = remote_host.clone();

                    let h = thread::spawn(move || {
                        eprintln!(
                            "[SSH] new connection → opening session to {ssh_host2}:{ssh_port}"
                        );
                        let session = match new_ssh_session(
                            &ssh_host2,
                            ssh_port,
                            &ssh_user2,
                            &ssh_password2,
                            &ssh_key2,
                        ) {
                            Ok(s) => s,
                            Err(e) => {
                                eprintln!("[SSH] session failed: {e}");
                                return;
                            }
                        };
                        eprintln!(
                            "[SSH] session ok → opening channel to {remote_host2}:{remote_port}"
                        );
                        forward_connection(session, local_stream, &remote_host2, remote_port);
                        eprintln!("[SSH] channel closed for {remote_host2}:{remote_port}");
                    });
                    connection_threads.push(h);
                    connection_threads.retain(|h| !h.is_finished());
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }

        for h in connection_threads {
            let _ = h.join();
        }
    });

    let tunnel = TemporaryTunnel {
        local_port,
        stop_signal: stop_tx,
        thread_handle: Some(handle),
    };

    resources.tunnels.insert(cache_key, tunnel);
    Ok(("127.0.0.1".to_owned(), local_port))
}

/// Forward one local TCP connection through a dedicated SSH channel.
///
/// Uses libssh2 non-blocking mode with session.block_directions() to
/// correctly multiplex reads and writes on the same channel without a mutex.
/// This is the approach recommended by the libssh2 documentation and avoids
/// the deadlock where a blocking read prevents writes during TLS handshake.
fn forward_connection(session: Session, local: TcpStream, remote_host: &str, remote_port: u16) {
    // Everything in blocking mode — no spin loops, no select(), zero idle CPU.
    //
    // The deadlock problem (Thread A holds channel mutex while blocking on read,
    // preventing Thread B from writing) is solved with an intermediate pipe:
    //
    //   Thread L→C : local.read()  →  pipe_in  (blocks on local socket)
    //   Main loop  : pipe_in.read() → channel.write()   ─┐ alternating,
    //              : channel.read() → pipe_out.write()   ─┘ no mutex needed
    //   Thread C→L : pipe_out.read() → local.write()   (blocks on pipe)
    //
    // The channel is only ever touched by the main loop on a single thread,
    // so there is no shared state and no locking at all.
    session.set_blocking(true);
    local.set_nonblocking(false).ok();

    let mut channel = match session.channel_direct_tcpip(remote_host, remote_port, None) {
        Ok(ch) => ch,
        Err(e) => {
            eprintln!("[SSH] channel_direct_tcpip failed: {e}");
            return;
        }
    };

    // Create two OS pipes as intermediaries.
    let (mut pipe_in_r, mut pipe_in_w) = match os_pipe() {
        Ok(p) => p,
        Err(_) => return,
    };
    let (mut pipe_out_r, mut pipe_out_w) = match os_pipe() {
        Ok(p) => p,
        Err(_) => return,
    };

    // Clone local socket for the writer thread
    let mut local_r = local;
    let mut local_w = match local_r.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Thread 1: local → pipe_in  (pure blocking copy, zero CPU when idle)
    let t_local_to_pipe = thread::spawn(move || {
        let mut buf = [0u8; 32768];
        loop {
            match local_r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if pipe_in_w.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        // Closing the write end signals EOF to the main loop's pipe_in read
    });

    // Thread 2: pipe_out → local  (pure blocking copy, zero CPU when idle)
    let t_pipe_to_local = thread::spawn(move || {
        let mut buf = [0u8; 32768];
        loop {
            match pipe_out_r.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if local_w.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = local_w.shutdown(std::net::Shutdown::Both);
    });

    // Main loop: shuttle between channel and the two pipes.
    // session.set_timeout gives the channel read a deadline so we can also
    // service the client→server direction without spinning.
    let mut lbuf = [0u8; 32768];
    let mut cbuf = [0u8; 32768];
    // 50 ms timeout on libssh2 blocking calls — low CPU, low latency
    session.set_timeout(50);

    use std::os::unix::io::AsRawFd;
    let pipe_in_fd = pipe_in_r.as_raw_fd();

    loop {
        // channel → pipe_out  (server → client)
        // session.set_timeout makes this return after 50 ms if no data
        match channel.read(&mut cbuf) {
            Ok(0) => {}
            Ok(n) => {
                if pipe_out_w.write_all(&cbuf[..n]).is_err() {
                    break;
                }
            }
            Err(e)
                if e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => break,
        }

        if channel.eof() {
            break;
        }

        // pipe_in → channel  (client → server)
        // Use poll() with 0ms timeout so we don't block if no client data
        let ready = {
            let mut pfd = libc::pollfd {
                fd: pipe_in_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            unsafe { libc::poll(&mut pfd, 1, 0) > 0 && (pfd.revents & libc::POLLIN) != 0 }
        };

        if ready {
            match pipe_in_r.read(&mut lbuf) {
                Ok(0) => break, // local closed
                Ok(n) => {
                    let mut written = 0;
                    while written < n {
                        match channel.write(&lbuf[written..n]) {
                            Ok(w) => written += w,
                            Err(_) => break,
                        }
                    }
                    let _ = channel.flush();
                }
                Err(_) => break,
            }
        }
    }

    // Drop pipe ends to unblock the helper threads
    drop(pipe_in_r);
    drop(pipe_out_w);

    let _ = channel.send_eof();
    let _ = channel.wait_eof();
    let _ = channel.close();
    let _ = channel.wait_close();

    let _ = t_local_to_pipe.join();
    let _ = t_pipe_to_local.join();
}

fn os_pipe() -> Result<(std::fs::File, std::fs::File), ()> {
    use std::os::unix::io::FromRawFd;
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(());
    }
    let r = unsafe { std::fs::File::from_raw_fd(fds[0]) };
    let w = unsafe { std::fs::File::from_raw_fd(fds[1]) };
    Ok((r, w))
}

fn find_free_local_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("port bind failed: {}", error))?;
    listener
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|error| format!("local addr failed: {}", error))
}

fn expand_tilde(path: &str) -> String {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return format!("{}/{}", home, stripped);
        }
    }

    path.to_owned()
}
