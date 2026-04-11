fn demo_history() -> Vec<HistoryEntry> {
    vec![
        HistoryEntry {
            title: "Recent paid orders".to_owned(),
            sql: "SELECT id, customer_email, total_cents\nFROM public.orders\nWHERE status = 'paid'\nORDER BY created_at DESC\nLIMIT 20;".to_owned(),
            summary: "Returned 20 rows from Production Cluster".to_owned(),
        },
        HistoryEntry {
            title: "Session duration breakdown".to_owned(),
            sql: "SELECT device, AVG(duration_sec)\nFROM analytics.sessions\nGROUP BY device\nORDER BY AVG(duration_sec) DESC;".to_owned(),
            summary: "Grouped sessions by device".to_owned(),
        },
    ]
}

fn demo_bookmarks() -> Vec<SavedQuery> {
    vec![
        SavedQuery {
            name: "High value orders".to_owned(),
            description: "Orders above 100 dollars".to_owned(),
            sql: "SELECT id, customer_email, total_cents, created_at\nFROM public.orders\nWHERE total_cents > 10000\nORDER BY created_at DESC\nLIMIT 50;".to_owned(),
        },
        SavedQuery {
            name: "Pending invoices".to_owned(),
            description: "Billing follow-up queue".to_owned(),
            sql: "SELECT invoice_id, customer, amount_usd, state\nFROM finance.invoices\nWHERE state = 'pending'\nORDER BY issued_at DESC;".to_owned(),
        },
    ]
}

fn demo_snippets() -> Vec<QuerySnippet> {
    vec![
        QuerySnippet {
            name: "Pagination template".to_owned(),
            description: "Offset pagination starter".to_owned(),
            body: "SELECT *\nFROM public.orders\nORDER BY created_at DESC\nLIMIT 100 OFFSET 0;".to_owned(),
        },
        QuerySnippet {
            name: "Health check".to_owned(),
            description: "Quick connection validation".to_owned(),
            body: "SELECT NOW() AS server_time, COUNT(*) AS total_rows\nFROM public.orders;".to_owned(),
        },
        QuerySnippet {
            name: "Aggregate revenue".to_owned(),
            description: "Group totals by status".to_owned(),
            body: "SELECT status, SUM(total_cents) AS revenue_cents\nFROM public.orders\nGROUP BY status\nORDER BY revenue_cents DESC;".to_owned(),
        },
    ]
}

fn demo_connections() -> Vec<ConnectionProfile> {
    Vec::new()
}

