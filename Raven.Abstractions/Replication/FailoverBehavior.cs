using System;

namespace Raven.Abstractions.Replication
{
    /// <summary>
    /// Options for handling failover scenarios in replication environment
    /// </summary>
    [Flags]
    public enum FailoverBehavior
    {
        /// <summary>
        /// Allow to read from the secondary server(s), but immediately fail writes
        /// to the secondary server(s).
        /// </summary>
        /// <remarks>
        /// This is usually the safest approach, because it means that you can still serve
        /// read requests when the primary node is down, but don't have to deal with replication
        /// conflicts if there are writes to the secondary when the primary node is down.
        /// </remarks>
        AllowReadsFromSecondaries = 1,

        /// <summary>
        /// Allow reads from and writes to secondary server(s).
        /// </summary>
        /// <remarks>
        /// Choosing this option requires that you'll have some way of propagating changes
        /// made to the secondary server(s) to the primary node when the primary goes back
        /// up. 
        /// A typical strategy to handle this is to make sure that the replication is setup
        /// in a master/master relationship, so any writes to the secondary server will be 
        /// replicated to the master server.
        /// Please note, however, that this means that your code must be prepared to handle
        /// conflicts in case of different writes to the same document across nodes.
        /// </remarks>
        AllowReadsFromSecondariesAndWritesToSecondaries = 3,

        /// <summary>
        /// Allow read from secondaries when request time SLA threshold is reached (configurable in conventions). Average request time is calculated using 60 second exponentially-weighted moving average (EWMA).
        /// </summary>
        AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached = 5,

        /// <summary>
        /// Immediately fail the request, without attempting any failover. This is true for both 
        /// reads and writes. The RavenDB client will not even check that you are using replication.
        /// </summary>
        /// <remarks>
        /// This is mostly useful when your replication setup is meant to be used for backups / external
        /// needs, and is not meant to be a failover storage.
        /// </remarks>
        FailImmediately = 0,

        /// <summary>
        /// Read requests will be spread across all the servers, instead of doing all the work against the master.
        /// Write requests will always go to the master.
        /// </summary>
        /// <remarks>
        /// This is useful for striping, spreading the read load among multiple servers. The idea is that this will give us better read performance overall.
        /// A single session will always use the same server, we don't do read striping within a single session.
        /// Note that using this means that you cannot set UserOptimisticConcurrency to true, because that would generate concurrency exceptions.
        /// If you want to use that, you have to open the session with ForceReadFromMaster set to true.
        /// </remarks>
        ReadFromAllServers = 1024,

        /// <summary>
        /// Cluster Behavior:
        /// Allows read from leader and write only to leader
        /// </summary>
        ReadFromLeaderWriteToLeader = 8,

        /// <summary>
        /// Cluster Behavior:
        /// Allows read from leader and write only to leader with failovers
        /// </summary>
        ReadFromLeaderWriteToLeaderWithFailovers = 8+32,

        /// <summary>
        /// Cluster Behavior:
        /// Allows read from any server and write only to leader
        /// </summary>
        ReadFromAllWriteToLeader = 16,

        /// <summary>
        /// Cluster Behavior:
        /// Allows read from any server and write only to leader with failovers
        /// </summary>
        ReadFromAllWriteToLeaderWithFailovers = 16+32

   
    }
}
