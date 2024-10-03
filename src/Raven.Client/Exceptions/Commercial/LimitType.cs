﻿using System.ComponentModel;

namespace Raven.Client.Exceptions.Commercial
{
    public enum LimitType
    {
        [Description("Invalid License")]
        InvalidLicense,

        [Description("Forbidden Host")]
        ForbiddenHost,

        [Description("Dynamic Nodes Distribution")]
        DynamicNodeDistribution,

        [Description("Cluster Size")]
        ClusterSize,

        [Description("Snapshot Backup")]
        SnapshotBackup,

        [Description("Cloud Backup")]
        CloudBackup,

        [Description("Encryption")]
        Encryption,

        [Description("Documents Compression")]
        DocumentsCompression,

        [Description("Rolling Indexes")]
        RollingIndexes,

        [Description("External Replication")]
        ExternalReplication,

        [Description("Raven ETL")]
        RavenEtl,

        [Description("SQL ETL")]
        SqlEtl,
        
        [Description("OLAP ETL")]
        OlapEtl,

        [Description("ElasticSearch ETL")]
        ElasticSearchEtl,

        [Description("Queue ETL")]
        QueueEtl,
        
        [Description("Snowflake ETL")]
        SnowflakeEtl,


        [Description("Cores Limit")]
        Cores,

        [Description("SNMP")]
        Snmp,

        [Description("PostgreSql Integration")]
        PostgreSqlIntegration,

        [Description("Power BI")]
        PowerBI,

        [Description("Delayed External Replication")]
        DelayedExternalReplication,

        [Description("Highly Available Tasks")]
        HighlyAvailableTasks,

        [Description("Pull Replication As Hub")]
        PullReplicationAsHub,

        [Description("Pull Replication As Sink")]
        PullReplicationAsSink,

        [Description("Time Series Rollups and Retention")]
        TimeSeriesRollupsAndRetention,

        [Description("Encrypted Backup")]
        EncryptedBackup,

        [Description("Additional Assemblies from NuGet")]
        AdditionalAssembliesFromNuGet,
        
        [Description("Monitoring Endpoints")]
        MonitoringEndpoints,

        [Description("Read-only Certificates")]
        ReadOnlyCertificates,

        [Description("Concurrent Subscriptions")]
        ConcurrentSubscriptions,

        [Description("TCP Data Compression")]
        TcpDataCompression,
        
        [Description("Server Wide Backups")]
        ServerWideBackups,

        [Description("Server Wide External Replications")]
        ServerWideExternalReplications,

        [Description("Server Wide Custom Sorters")]
        ServerWideCustomSorters,

        [Description("Server Wide Analyzers")]
        ServerWideAnalyzers,

        [Description("Index Cleanup")]
        IndexCleanup,

        [Description("Periodic Backup")]
        PeriodicBackup,

        [Description("Client Configuration")]
        ClientConfiguration,

        [Description("Studio Configuration")]
        StudioConfiguration,

        [Description("Queue Sink")]
        QueueSink,

        [Description("Data Archival")]
        DataArchival,

        [Description("Sharding")]
        Sharding,

        [Description("Subscriptions")]
        Subscriptions,

        [Description("Revisions Configuration")]
        RevisionsConfiguration,

        [Description("Expiration")]
        Expiration,

        [Description("Refresh")]
        Refresh,

        [Description("Indexes")]
        Indexes,

        [Description("Custom Sorters")]
        CustomSorters,

        [Description("Custom Analyzers")]
        CustomAnalyzers
    }
}
