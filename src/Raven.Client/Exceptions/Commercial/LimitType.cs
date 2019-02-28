using System.ComponentModel;

namespace Raven.Client.Exceptions.Commercial
{
    public enum LimitType
    {
        [Description("Invalid License")]
        InvalidLicense,

        [Description("Forbidden Downgrade")]
        ForbiddenDowngrade,

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

        [Description("External Replication")]
        ExternalReplication,

        [Description("Raven ETL")]
        RavenEtl,

        [Description("SQL ETL")]
        SqlEtl,

        [Description("Cores Limit")]
        Cores,

        [Description("Downgrade")]
        Downgrade,

        [Description("SNMP")]
        Snmp,

        [Description("Delayed External Replication")]
        DelayedExternalReplication,

        [Description("Highly Available Tasks")]
        HighlyAvailableTasks,

        [Description("Pull Replication As Hub")]
        PullReplicationAsHub,

        [Description("Pull Replication As Sink")]
        PullReplicationAsSink
    }
}
