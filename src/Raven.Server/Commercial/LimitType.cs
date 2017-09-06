namespace Raven.Server.Commercial
{
    public enum LimitType
    {
        InvalidLicense,
        ForbiddenDowngrade,
        ForbiddenHost,
        DynamicNodeDistribution,
        ClusterSize,
        SnapshotBackup,
        CloudBackup,
        Encryption,
        ExternalReplication,
        RavenEtl,
        SqlEtl,
        Cores,
        Memory,
        Downgrade
    }
}
