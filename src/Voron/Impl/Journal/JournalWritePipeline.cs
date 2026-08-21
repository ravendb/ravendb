using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Global;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron.Impl.Journal;

internal sealed unsafe class JournalWritePipeline : IDisposable
{
    public const int MaxPipelinedBatchSizeInBytes = Constants.Size.Megabyte;

    private const int MaxPipelinedBatch4Kbs = MaxPipelinedBatchSizeInBytes / Constants.Storage.JournalPageSize;

    internal readonly record struct Ack(StorageEnvironment Environment, long TransactionId, TaskCompletionSource CommitCompleted);

    private sealed class PendingWrite(JournalWritePipeline pipeline) : IThreadPoolWorkItem, IDisposable
    {
        public readonly List<Ack> Acks = [];
        public SafeJournalWriteContext Context;
        public JournalFile File;
        public long PosBy4Kb;
        public byte* Buffer;
        public long BufferSize;
        public NativeMemory.ThreadStats BufferAllocatingThread;
        public long BufferGeneration;
        public int NumberOf4Kbs;
        public long Sequence;
        public Exception Error;

        public void Reset()
        {
            Acks.Clear();
            File = null;
            PosBy4Kb = 0;
            Buffer = null;
            BufferSize = 0;
            BufferAllocatingThread = null;
            BufferGeneration = 0;
            NumberOf4Kbs = 0;
            Sequence = 0;
            Error = null;
        }

        public void Execute() => pipeline.Execute(this);

        public void Dispose() => Context?.Dispose();
    }

    private readonly StorageEnvironment _env;
    private readonly EncryptionBuffersPool _buffers;
    private readonly int _maxConcurrentWrites;
    private readonly PendingWrite[] _slots;

    private ulong _claimedSlots;
    private readonly ulong _allSlots;
    private ulong _completedSlots;
    private long _nextSequence;    // submitter only, serialized by the journal write lock
    private long _reapedSequence;  // claimed by reapers with a CAS
    private long _lowestFailedSequence = long.MaxValue;
    private ExceptionDispatchInfo _failure;
    private readonly object _waiters = new();
    private volatile bool _disposed;

    public JournalWritePipeline(StorageEnvironment env)
    {
        _env = env;
        _buffers = env.Options.Encryption.EncryptionBuffersPool;
        _maxConcurrentWrites = Math.Clamp(env.Options.MaxConcurrentJournalWrites, 1, StorageEnvironmentOptions.MaxSupportedConcurrentJournalWrites);
        _slots = new PendingWrite[_maxConcurrentWrites];
        _allSlots = _maxConcurrentWrites == 64 ? ulong.MaxValue : (1UL << _maxConcurrentWrites) - 1;
    }

    public bool IsPipelining => _maxConcurrentWrites > 1;

    public int MaxConcurrentWrites => _maxConcurrentWrites;

    public bool CanPipeline(long totalNumberOf4Kbs) => IsPipelining && totalNumberOf4Kbs <= MaxPipelinedBatch4Kbs;

    public void SubmitPipelined(JournalFile file, long posBy4Kb, Span<Pal.journal_entry> entries, long totalNumberOf4Kbs, List<Ack> acks)
    {
        // for this to make sense, we have to copy the data to *our own buffer*, if this is too large, the caller should use WriteInline instead
        if (CanPipeline(totalNumberOf4Kbs) == false)
            throw new InvalidOperationException(
                $"Refusing to pipeline a journal write of {totalNumberOf4Kbs} 4KB blocks (limit is {MaxPipelinedBatch4Kbs}, pipelining enabled: {IsPipelining}) - such a write must go through {nameof(WriteInline)}");


        var write = RentWrite(file, posBy4Kb, (int)totalNumberOf4Kbs, acks);

        file.AddRef();

        try
        {
            RentBuffer(write);

            long copied4Kbs = 0;
            foreach (var entry in entries)
            {
                Debug.Assert(copied4Kbs + entry.NumberOf4Kbs <= write.NumberOf4Kbs);

                Memory.Copy(write.Buffer + copied4Kbs * Constants.Storage.JournalPageSize, (byte*)entry.Base,
                    entry.NumberOf4Kbs * Constants.Storage.JournalPageSize);
                copied4Kbs += entry.NumberOf4Kbs;
            }

            Debug.Assert(copied4Kbs == write.NumberOf4Kbs, $"copied {copied4Kbs} of {write.NumberOf4Kbs} 4KB blocks");

            NoteSubmitted(write);

            ThreadPool.UnsafeQueueUserWorkItem(write, preferLocal: false);
        }
        catch (Exception e)
        {
            write.Error = e;
            Complete(write);
            throw;
        }
    }

    public void WriteInline(JournalFile file, long posBy4Kb, Span<Pal.journal_entry> entries, long totalNumberOf4Kbs, List<Ack> acks)
    {
        // we mustn't have anything else concurrently running with us
        Drain(throwOnFailure: false);

        var write = RentWrite(file, posBy4Kb, checked((int)totalNumberOf4Kbs), acks);

        file.AddRef();

        try
        {
            NoteSubmitted(write);

            if (IsAfterFailure(write.Sequence) == false) // otherwise, Complete will raise the error
                file.Write(posBy4Kb, entries, write.Context);
        }
        catch (Exception e)
        {
            write.Error = e;
        }

        Complete(write);

        Volatile.Read(ref _failure)?.Throw();
    }

    public void Drain(bool throwOnFailure = true)
    {
        while (_disposed == false && Volatile.Read(ref _claimedSlots) != 0)
            WaitForSlots(_allSlots);

        if (throwOnFailure)
            Volatile.Read(ref _failure)?.Throw();
    }

    private void AcquireAllSlots()
    {
        while (Interlocked.CompareExchange(ref _claimedSlots, _allSlots, 0UL) != 0UL)
            WaitForSlots(_allSlots);
    }

    private void WaitForSlots(ulong mask)
    {
        lock (_waiters)
        {
            if ((Volatile.Read(ref _claimedSlots) & mask) == 0)
                return;

            Monitor.Wait(_waiters);
        }
    }

    private void SlotsChanged()
    {
        lock (_waiters)
            Monitor.PulseAll(_waiters);
    }

    private static void NoteSubmitted(PendingWrite write)
    {
        foreach (var ack in write.Acks)
        {
            ack.Environment.NoteJournalWriteSubmitted(ack.TransactionId);
        }
    }

    private bool IsAfterFailure(long sequence) => sequence > Volatile.Read(ref _lowestFailedSequence);

    private void Execute(PendingWrite write)
    {
        try
        {
            if (IsAfterFailure(write.Sequence) == false)
            {
                var entry = new Pal.journal_entry { Base = write.Buffer, NumberOf4Kbs = write.NumberOf4Kbs };
                write.File.Write(write.PosBy4Kb, MemoryMarshal.CreateSpan(ref entry, 1), write.Context);
            }
            else
            {
                // Complete will raise this error
            }
        }
        catch (Exception e)
        {
            write.Error = e;
        }
        finally
        {
            Complete(write);
        }
    }

    private void MarkCompleted(PendingWrite write)
    {
        if (write.Error != null)
        {
            Interlocked.CompareExchange(ref _failure, ExceptionDispatchInfo.Capture(write.Error), null);

            ThreadingHelper.InterlockedExchangeMin(ref _lowestFailedSequence, write.Sequence);
        }

        Interlocked.Or(ref _completedSlots, SlotMask(write.Sequence));
    }

    private void Complete(PendingWrite write)
    {
        MarkCompleted(write);

        while (true)
        {
            var reaped = Volatile.Read(ref _reapedSequence);
            var completed = Volatile.Read(ref _completedSlots);

            var run = 0;
            while (run < _maxConcurrentWrites && (completed & SlotMask(reaped + run)) != 0)
                run++;

            if (run == 0)
                return; // the head is still in flight, whoever finishes it will pick us up

            if (Interlocked.CompareExchange(ref _reapedSequence, reaped + run, reaped) != reaped)
                continue; // someone else claimed part of this run, look again

            for (var i = 0; i < run; i++)
            {
                Reap(_slots[(reaped + i) % _maxConcurrentWrites]);
            }
        }
    }

    private void Reap(PendingWrite write)
    {
        try
        {
            if (write.Sequence >= Volatile.Read(ref _lowestFailedSequence))
                FailAcks(write);
            else
                CompleteAcks(write);
        }
        finally
        {
            write.File.Release();

            ReturnBuffer(write);

            var slot = SlotMask(write.Sequence);

            write.Reset();

            ReleaseSlot(slot);
        }
    }

    private ulong SlotMask(long sequence) => 1UL << (int)(sequence % _maxConcurrentWrites);

    private PendingWrite RentWrite(JournalFile file, long posBy4Kb, int numberOf4Kbs, List<Ack> acks)
    {
        var sequence = _nextSequence;
        var slot = SlotMask(sequence);

        var pending = _slots[sequence % _maxConcurrentWrites] ??= new PendingWrite(this);
        pending.Context ??= SafeJournalWriteContext.Create();

        while (true)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(JournalWritePipeline), "Cannot submit a journal write, the pipeline is being disposed");

            var claimed = Volatile.Read(ref _claimedSlots);
            if ((claimed & slot) != 0)
            {
                WaitForSlots(slot);
                continue;
            }

            if (Interlocked.CompareExchange(ref _claimedSlots, claimed | slot, claimed) == claimed)
                break;
        }

        Debug.Assert(pending.Acks.Count == 0, "the slot was left dirty by its previous write");
        Debug.Assert(pending.File == null, "the slot was left dirty by its previous write");
        Debug.Assert(pending.Buffer == null, "the slot was left dirty by its previous write");

        try
        {
            pending.Sequence = sequence;
            pending.File = file;
            pending.PosBy4Kb = posBy4Kb;
            pending.NumberOf4Kbs = numberOf4Kbs;
            pending.Acks.AddRange(acks);
        }
        catch
        {
            pending.Reset();
            ReleaseSlot(slot);
            throw;
        }

        _nextSequence = sequence + 1;

        return pending;
    }

    private void ReleaseSlot(ulong slot)
    {
        Interlocked.And(ref _completedSlots, ~slot);
        Interlocked.And(ref _claimedSlots, ~slot);

        SlotsChanged();
    }

    private static void CompleteAcks(PendingWrite write)
    {
        foreach (var ack in write.Acks)
        {
            ack.Environment.MarkJournalWriteDurable(ack.TransactionId);
            ack.CommitCompleted?.TrySetResult();
        }
    }

    private void FailAcks(PendingWrite write)
    {
        var error = Volatile.Read(ref _failure);

        MarkFailed(_env, error);

        foreach (var ack in write.Acks)
        {
            MarkFailed(ack.Environment, error);
            ack.CommitCompleted?.TrySetException(error.SourceException);
        }
    }

    private static void MarkFailed(StorageEnvironment environment, ExceptionDispatchInfo error)
    {
        try
        {
            environment.Options.SetCatastrophicFailure(error);
        }
        catch
        {
        }

        environment.MarkJournalWriteFailed(error);
    }

    private void RentBuffer(PendingWrite write)
    {
        var numberOfPages = checked((int)((write.NumberOf4Kbs * Constants.Storage.JournalPageSize + Constants.Storage.PageSize - 1) / Constants.Storage.PageSize));

        write.BufferGeneration = _buffers.Generation;
        write.Buffer = _buffers.Get(numberOfPages, out write.BufferSize, out write.BufferAllocatingThread);
    }

    private void ReturnBuffer(PendingWrite write)
    {
        if (write.Buffer == null)
            return;

        _buffers.Return(write.Buffer, write.BufferSize, write.BufferAllocatingThread, write.BufferGeneration);

        write.Buffer = null;
    }

    private TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly()
    {
        if (_forTestingPurposes != null)
            return _forTestingPurposes;

        return _forTestingPurposes = new TestingStuff(this);
    }

    internal sealed class TestingStuff(JournalWritePipeline pipeline)
    {
        // approximate - the reap cursor moves ahead of the acks being delivered, so this can read low
        internal int InFlightCount => (int)(Volatile.Read(ref pipeline._nextSequence) - Volatile.Read(ref pipeline._reapedSequence));
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        AcquireAllSlots();

        _disposed = true;

        // the slots are deliberately left claimed - releasing them would let a submitter that already passed the
        // disposed check claim one and use a context we are about to dispose. Waiters are woken to observe it
        SlotsChanged();

        foreach (var write in _slots)
            write?.Dispose();
    }
}
