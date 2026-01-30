namespace Raven.Server.NotificationCenter.Notifications
{
    public enum PerformanceHintReason
    {
        None = 0,
        Indexing = 1,
        Replication = 2,
        Paging = 3,
        RequestLatency = 4,
        UnusedCapacity = 5,
        SlowIO = 6,
        SqlEtl_SlowSql = 7,
        HugeDocuments = 8,
        Indexing_References = 9
    }
}
