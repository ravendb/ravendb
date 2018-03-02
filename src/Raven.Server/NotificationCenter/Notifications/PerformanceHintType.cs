namespace Raven.Server.NotificationCenter.Notifications
{
    public enum PerformanceHintType
    {
        None,
        Indexing,
        Replication,
        Paging,
        RequestLatency,
        UnusedCapacity,
        SlowIO,
        SqlEtl_SlowSql
    }
}
