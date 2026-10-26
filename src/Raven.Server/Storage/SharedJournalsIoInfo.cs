using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow;
using Voron;
using Voron.Impl.Journal;

namespace Raven.Server.Storage
{
    /// <summary>
    /// Index environments of a database write through one shared journal, hard-linked into each index (see SharedIndexJournals).
    /// </summary>
    public sealed class SharedJournalsIoInfo
    {
        public string Database { get; set; }

        public List<SharedJournalFileInfo> Files { get; set; }

        public int IndexesSharing { get; set; }

        public List<IndexNotSharingInfo> IndexesNotSharing { get; set; }

        // index commits waiting for the shared journal writer right now
        public int MergedCommitsQueue { get; set; }

        // average time from an index commit handing its entry to the shared journal until it is durable, 0 when idle
        public double CommitToDurableInMs { get; set; }

        // the index link that keeps the oldest shared journal file on disk, null when no index holds a link
        public SharedJournalRetentionInfo RetainedBy { get; set; }

        // hard-linked journals recovered at startup: their shared journal is not known until they are recycled
        public int LinksOfUnknownSource { get; set; }

        public static SharedJournalsIoInfo Create(string database, List<StorageDeviceMonitor.EnvironmentOfDatabase> environments)
        {
            StorageDeviceMonitor.EnvironmentOfDatabase root = environments.FirstOrDefault(e => e.Environment.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
            if (root.Environment == null)
                return null;

            WriteAheadJournal rootJournal = root.Environment.Environment.Journal;
            IReadOnlyDictionary<StorageEnvironment, DateTime> fallbacks = root.Instance?.IndexStore?.SharedJournals?.Fallbacks;

            var info = new SharedJournalsIoInfo
            {
                Database = database,
                Files = rootJournal.Files.Select(f => new SharedJournalFileInfo { Number = f.Number, SizeInBytes = f.JournalSize.GetValue(SizeUnit.Bytes) })
                    .Concat(rootJournal.Applicator.JournalsToDelete.Select(f => new SharedJournalFileInfo
                    {
                        Number = f.Number,
                        SizeInBytes = f.JournalSize.GetValue(SizeUnit.Bytes),
                        PendingDeletion = true
                    }))
                    .OrderBy(f => f.Number)
                    .ToList(),
                IndexesNotSharing = new List<IndexNotSharingInfo>(),
                MergedCommitsQueue = rootJournal.SharedJournalState.MergedCommitsQueue.Count,
                CommitToDurableInMs = rootJournal.BranchCommitToDurable.TotalMilliseconds
            };

            long oldestSharedJournal = info.Files.Count > 0 ? info.Files[0].Number : long.MaxValue;
            foreach (StorageDeviceMonitor.EnvironmentOfDatabase env in environments)
            {
                if (env.Environment.Type != StorageEnvironmentWithType.StorageEnvironmentType.Index)
                    continue;

                StorageEnvironment index = env.Environment.Environment;
                if (index.Options.RootJournal == rootJournal)
                {
                    info.IndexesSharing++;
                }
                else
                {
                    DateTime? since = fallbacks != null && fallbacks.TryGetValue(index, out DateTime at) ? at : null;
                    info.IndexesNotSharing.Add(new IndexNotSharingInfo
                    {
                        Index = env.Environment.Name,
                        Reason = since != null ? IndexNotSharingReason.HardLinkLimit : IndexNotSharingReason.NotLinked,
                        Since = since
                    });
                }

                foreach (JournalFile file in index.Journal.Files.Concat(index.Journal.Applicator.JournalsToDelete))
                {
                    if (file.IsHardLinked == false)
                        continue;

                    if (file.SourceJournalNumber < 0)
                    {
                        info.LinksOfUnknownSource++;
                        continue;
                    }

                    if (info.RetainedBy == null || file.SourceJournalNumber < info.RetainedBy.JournalNumber)
                    {
                        info.RetainedBy = new SharedJournalRetentionInfo
                        {
                            Index = env.Environment.Name,
                            JournalNumber = file.SourceJournalNumber,
                            AlreadyReleasedBySharedJournal = file.SourceJournalNumber < oldestSharedJournal
                        };
                    }
                }
            }

            return info;
        }
    }

    public sealed class SharedJournalFileInfo
    {
        public long Number { get; set; }

        public long SizeInBytes { get; set; }

        // synced, waiting for deletion, still on disk
        public bool PendingDeletion { get; set; }
    }

    public enum IndexNotSharingReason
    {
        NotLinked,    // never linked: shared journals disabled, or the index is on a volume the shared journal cannot hard link to
        HardLinkLimit // was sharing until the file system hard link limit was reached
    }

    public sealed class IndexNotSharingInfo
    {
        public string Index { get; set; }

        public IndexNotSharingReason Reason { get; set; }

        public DateTime? Since { get; set; }
    }

    public sealed class SharedJournalRetentionInfo
    {
        public string Index { get; set; }

        public long JournalNumber { get; set; }

        // the shared journal itself already let go of this file, only the index link keeps its disk space
        public bool AlreadyReleasedBySharedJournal { get; set; }
    }
}
