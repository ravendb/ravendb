using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Sparrow.Server.Platform;

namespace Voron.Impl.Journal;

public class SharedJournalState()
{
    private readonly ConcurrentQueue<WriteAheadJournal.PendingJournalStateRecord> _mergedCommitsQueue = new();
    private readonly List<WriteAheadJournal.JournalStateRecord> _mergedJournalJournalRecordsBuffer = new List<WriteAheadJournal.JournalStateRecord>();
    private readonly List<Pal.journal_entry> _mergedEntriesBuffer = new List<Pal.journal_entry>();
    public bool HasBranchCommits => _mergedCommitsQueue.IsEmpty is false;

    public void Enqueue(WriteAheadJournal.PendingJournalStateRecord record) => _mergedCommitsQueue.Enqueue(record);

    public bool TryDequeue(out WriteAheadJournal.PendingJournalStateRecord record) => _mergedCommitsQueue.TryDequeue(out record);

    public bool IsEmpty => _mergedCommitsQueue.IsEmpty;

    public Span<Pal.journal_entry> Entries => CollectionsMarshal.AsSpan(_mergedEntriesBuffer);
    public List<WriteAheadJournal.JournalStateRecord> JournalRecords => _mergedJournalJournalRecordsBuffer;
    public ConcurrentQueue<WriteAheadJournal.PendingJournalStateRecord> MergedCommitsQueue => _mergedCommitsQueue;

    public void PrepareForCommit(WriteAheadJournal.JournalStateRecord state)
    {
        _mergedJournalJournalRecordsBuffer.Add(state);
        _mergedEntriesBuffer.Add(state.Entry);
    }

    public void PrepareForCommit(Pal.journal_entry entry)
    {
        _mergedEntriesBuffer.Add(entry);
    }

    public void Reset()
    {
        _mergedJournalJournalRecordsBuffer.Clear();
        _mergedEntriesBuffer.Clear();
    }

    public void SetException(Exception e)
    {
        while (_mergedCommitsQueue.TryDequeue(out var rec))
        {
            rec.Tcs.TrySetException(e);
        }

        foreach (var record in _mergedJournalJournalRecordsBuffer)
        {
            record.Tcs.TrySetException(e);
        }
    }

    public void SetCancel()
    {
        while (_mergedCommitsQueue.TryDequeue(out var rec))
        {
            rec.Tcs.TrySetCanceled();
        }

        foreach (var record in _mergedJournalJournalRecordsBuffer)
        {
            record.Tcs.TrySetCanceled();
        }
    }
}
