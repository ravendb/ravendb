using System;
using System.IO;
using System.Threading;
using Rachis.Commands;
using Rachis.Messages;

namespace Rachis.Interfaces
{
    public interface IRaftStateMachine : IDisposable
    {
        /// <summary>
        /// This is a thread safe operation, since this is being used by both the leader's message processing thread
        /// and the leader's heartbeat thread
        /// </summary>
        long LastAppliedIndex { get; }

        void Apply(LogEntry entry, Command cmd);

        bool SupportSnapshots { get; }

        /// <summary>
        /// Create a snapshot, can be called concurrently with GetSnapshotWriter, can also be called concurrently
        /// with calls to Apply.
        /// </summary>
        void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications);

        /// <summary>
        /// Can be called concurrently with CreateSnapshot
        /// Should be cheap unless WriteSnapshot is called
        /// </summary>
        ISnapshotWriter GetSnapshotWriter();

        /// <summary>
        /// Nothing else may access the state machine when this is running, this is guranteed by Raft.
        /// </summary>
        void ApplySnapshot(long term, long index, Stream stream);

        /// <summary>
        /// this method is intended to be use for emergency fixes of the cluster state using the JS admin console
        /// it will set the last applied index to the given posion so we can replay the log 
        /// </summary>
        void Danger__SetLastApplied(long postion);
    }
}
