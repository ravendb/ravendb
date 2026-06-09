namespace Raven.Server.Documents.Replication
{
    public enum PullReplicationChangeVectorShape
    {
        Flat,
        Composite
    }

    public enum PullReplicationChangeVectorTransmission
    {
        SendAsIs,
        SendVersionOnly
    }

    internal static class PullReplicationChangeVectorModeSelector
    {
        internal static PullReplicationChangeVectorTransmission GetChangeVectorTransmission(bool localSupportsCompositeChangeVectors, bool remoteSupportsCompositeChangeVectors)
        {
            return localSupportsCompositeChangeVectors && remoteSupportsCompositeChangeVectors
                ? PullReplicationChangeVectorTransmission.SendAsIs
                : PullReplicationChangeVectorTransmission.SendVersionOnly;
        }

        internal static PullReplicationChangeVectorShape GetChangeVectorShape(bool canFilterOutSourceItems, PullReplicationChangeVectorTransmission transmission)
        {
            return transmission switch
            {
                PullReplicationChangeVectorTransmission.SendAsIs when canFilterOutSourceItems => PullReplicationChangeVectorShape.Composite,
                PullReplicationChangeVectorTransmission.SendAsIs => PullReplicationChangeVectorShape.Flat,
                PullReplicationChangeVectorTransmission.SendVersionOnly => PullReplicationChangeVectorShape.Flat,
                _ => throw new System.ArgumentOutOfRangeException(nameof(transmission), transmission, "Unknown pull replication change-vector transmission.")
            };
        }
    }
}
