using System.Collections.Generic;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Stats;
using Voron.Data.Fixed;

namespace Raven.Server.Documents.Replication.Stats
{
    public class ReplicationTaskProgress : IReplicationTaskProgress
    {
        public string TaskName { get; set; }

        public ReplicationNode.ReplicationType ReplicationType { get; set; }

        public List<ReplicationProcessProgress> ProcessesProgress { get; set; }
    }

    public sealed class InternalReplicationTaskProgress : ReplicationTaskProgress
    {
        public string DestinationNodeTag { get; set; }
    }

    public sealed class ReplicationProcessProgress : ProcessProgress
    {
        public string FromToString { get; set; }

        public string HandlerId { get; set; }

        public long LastSentEtag { get; set; }

        public long LastDatabaseEtag { get; set; }

        public string DestinationChangeVector { get; set; }

        public string SourceChangeVector { get; set; }

        public long NumberOfRevisionsToProcess { get; set; }

        public long TotalNumberOfRevisions { get; set; }

        public long TotalNumberOfRevisionTombstones { get; set; }

        public long NumberOfAttachmentsToProcess { get; set; }

        public long TotalNumberOfAttachments { get; set; }

        public long TotalNumberOfAttachmentTombstones { get; set; }

        public void AddRevisions(in NumberOfEntriesAfterResult result)
        {
            NumberOfRevisionsToProcess += result.Count;
            TotalNumberOfRevisions += result.Total;
            Estimated |= result.Estimated;
        }

        public void AddAttachments(in NumberOfEntriesAfterResult result)
        {
            NumberOfAttachmentsToProcess += result.Count;
            TotalNumberOfAttachments += result.Total;
            Estimated |= result.Estimated;
        }
    }
}
