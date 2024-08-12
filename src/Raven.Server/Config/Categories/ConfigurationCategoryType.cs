using System.ComponentModel;

namespace Raven.Server.Config.Categories
{
    public enum ConfigurationCategoryType
    {
        None,
        Backup,
        Cluster,
        Core,
        Database,
        Embedded,
        [Description("ETL")]
        Etl,
        Http,
        Indexing,
        Integrations,
        License,
        Logs,
        Memory,
        Migration,
        Monitoring,
        Notifications,
        Patching,
        [Description("Performance Hints")]
        PerformanceHints,
        Query,
        Replication,
        Security,
        Server,
        Sharding,
        Storage,
        Studio,
        Subscriptions,
        Tombstones,
        [Description("Transaction Merger")]
        TransactionMerger,
        Updates,
        [Description("Traffic Watch")]
        TrafficWatch,
        [Description("Queue Sink")]
        QueueSink,
        [Description("Export & Import")]
        ExportImport,
        Debug
    }
}
