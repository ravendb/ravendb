namespace Raven.Client.ServerWide.Sharding;

public enum MigrationStatus
{
    None,

    // source is in progress of sending the bucket
    Moving,

    // the source has completed to send everything he has
    // and the destinations member nodes start confirm having all docs
    // at this stage writes will still go to the source shard
    Moved,

    // all member nodes confirmed receiving the bucket
    // the mapping is updated so any traffic will go now to the destination
    // the source will start the cleanup process
    OwnershipTransferred
}
