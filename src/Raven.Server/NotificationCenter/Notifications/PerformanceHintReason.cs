namespace Raven.Server.NotificationCenter.Notifications
{
    public enum PerformanceHintReason
    {
        None,
        Indexing,
        Replication,
        Paging,
        RequestLatency,
        UnusedCapacity,
        SlowIO,
        SqlEtl_SlowSql,
        HugeDocuments,
        Indexing_References
    }
}
