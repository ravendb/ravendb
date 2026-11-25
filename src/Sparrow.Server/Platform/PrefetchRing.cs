using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Sparrow.Binary;
using Sparrow.Platform;

namespace Sparrow.Server.Platform;

[StructLayout(LayoutKind.Sequential)]
public readonly struct PrefetchRingStats
{
    public readonly long Submitted;
    public readonly long DroppedFull;
    public readonly long DroppedStale;
    public readonly long Failed;
    public readonly long ReadGroups;
    public readonly long Reads;
    private readonly long _usesIoUring;

    public bool UsesIoUring => _usesIoUring != 0;
}

/// <summary>
/// The PAL's read-ahead queue for data files, drained by io_uring, or by a posix_fadvise thread where io_uring is not
/// allowed. No ring (<see cref="IsAvailable"/> false) on Windows, macOS or with maxFiles 0. Dispose after its data files.
/// </summary>
public sealed class PrefetchRing : IDisposable
{
    private static readonly Lazy<PrefetchRing> LazyDefault = new(CreateDefault);

    /// <summary>The process's ring, created on first use. If it cannot be created, every use throws the same error.</summary>
    public static PrefetchRing Default => LazyDefault.Value;

    private static PrefetchRing CreateDefault()
    {
        try
        {
            return new PrefetchRing(PalConfiguration.PrefetchRingEntries,
                PalConfiguration.PrefetchMaxFiles ?? PalConfiguration.FilesTheProcessCanOpen(), PalConfiguration.PrefetchQueueSize);
        }
        catch (Exception e)
        {
            throw new InvalidOperationException(
                "Could not create the prefetch ring, which reads data files ahead. To run without read-ahead, set 'Storage.EnablePrefetching' to false.", e);
        }
    }

    private const int WaitSlots = 1024;

    private unsafe void* _handle;
    private GCHandle _self;
    private readonly bool _usesIoUring;
    private readonly WaitSlot[] _slots;
    private readonly ulong[] _busySlots;
    private readonly SemaphoreSlim _freeSlots;

    /// <summary>The PAL handle, passed to rvn_init_pager; null when there is no ring.</summary>
    public unsafe void* Handle => _handle;

    /// <summary>The number of data files that can be open with the ring at the same time.</summary>
    public int MaxFiles { get; }

    public unsafe bool IsAvailable => _handle != null;

    /// <summary>Without io_uring, the posix_fadvise thread cannot tell when a read completes.</summary>
    public bool CanWaitForPages => _usesIoUring;

    public unsafe PrefetchRing(int ringEntries, int maxFiles, int queueSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxFiles);
        MaxFiles = maxFiles;
        if (PlatformDetails.RunningOnLinux == false || maxFiles == 0)
            return;

        queueSize = Bits.PowerOf2(queueSize); // the PAL indexes the queue by mask
        _self = GCHandle.Alloc(this);
        var rc = Pal.rvn_prefetch_ring_create(ringEntries, maxFiles, queueSize, WaitSlots, &OnReadsDone, (void*)GCHandle.ToIntPtr(_self),
            out var handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
        {
            _self.Free();
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to create a prefetch ring ({ringEntries} entries, {maxFiles} files, queue of {queueSize})");
        }
        _handle = handle;

        _usesIoUring = GetStats()?.UsesIoUring == true;
        if (_usesIoUring == false)
            return;

        _slots = new WaitSlot[WaitSlots];
        for (var i = 0; i < _slots.Length; i++)
            _slots[i] = new WaitSlot(this, i);
        _busySlots = new ulong[WaitSlots / 64];
        _freeSlots = new SemaphoreSlim(WaitSlots);
    }

    /// <summary>For tests: the next <paramref name="count"/> submits fail with <paramref name="errno"/>, as the kernel would; transient errors for <paramref name="giveUpMs"/> fail the ring.</summary>
    internal unsafe void FailSubmitsForTesting(int count, int errno, int giveUpMs = 15_000)
    {
        if (_handle != null)
            Pal.rvn_prefetch_ring_fail_submits(_handle, count, errno, giveUpMs);
    }

    public unsafe PrefetchRingStats? GetStats()
    {
        if (_handle == null || Pal.rvn_prefetch_ring_stats(_handle, out var stats) != PalFlags.FailCodes.Success)
            return null;
        return stats;
    }

    /// <summary>Takes a wait slot if one is free. <see cref="WaitForPages"/> returns it; so does <see cref="ReturnSlot"/>.</summary>
    public bool TryRentSlot(out int slot)
    {
        slot = -1;
        if (_usesIoUring == false || _freeSlots.Wait(0) == false)
            return false;
        slot = TakeFreeSlot();
        return true;
    }

    /// <summary>Completes with a wait slot once one is free.</summary>
    public async Task<int> RentSlotAsync()
    {
        await _freeSlots.WaitAsync().ConfigureAwait(false);
        return TakeFreeSlot();
    }

    /// <summary>
    /// Queues the pages at <paramref name="offsets"/> of the file with <paramref name="ticket"/> on the rented <paramref name="slot"/>,
    /// and returns the slot to the ring once the ring's reads of them complete.
    /// </summary>
    public unsafe ValueTask WaitForPages(int slot, long ticket, ReadOnlySpan<long> offsets)
    {
        var waitSlot = _slots[slot];
        var version = waitSlot.Version;
        var queued = 0;
        var handle = _handle;
        if (handle != null)
        {
            fixed (long* p = offsets)
                queued = Pal.rvn_prefetch_ring_read_pages(handle, ticket, p, offsets.Length, (uint)slot);
        }
        if (queued != 1)
        {
            ReturnSlot(slot);
            return ValueTask.CompletedTask;
        }
        return new ValueTask(waitSlot, version);
    }

    // a free semaphore count means a clear bit exists
    private int TakeFreeSlot()
    {
        var words = _busySlots;
        var start = (uint)Environment.CurrentManagedThreadId; // spreads concurrent callers over the words
        for (var n = 0u; n < words.Length * 100; n++) // a pass can miss a bit freed behind it while others take those ahead
        {
            var w = (int)((start + n) % (uint)words.Length);
            var bits = Volatile.Read(ref words[w]);
            while (bits != ulong.MaxValue)
            {
                var bit = BitOperations.TrailingZeroCount(~bits);
                var seen = Interlocked.CompareExchange(ref words[w], bits | (1UL << bit), bits);
                if (seen == bits)
                    return w * 64 + bit;
                bits = seen;
            }
        }
        throw new InvalidOperationException("A wait slot was reserved, but no free slot was found");
    }

    public void ReturnSlot(int index)
    {
        Interlocked.And(ref _busySlots[index / 64], ~(1UL << (index % 64)));
        _freeSlots.Release();
    }

    // a slot's index is the PAL group index: it is not queued again until its callback and GetResult
    private sealed class WaitSlot(PrefetchRing ring, int index) : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };

        public short Version => _core.Version;

        public void Complete() => _core.SetResult(true);

        public void GetResult(short token)
        {
            _core.GetResult(token);
            _core.Reset();
            ring.ReturnSlot(index);
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);
    }

    // runs on the ring's submitter thread; the context is the ring's GCHandle
    [UnmanagedCallersOnly]
    private static unsafe void OnReadsDone(void* context, uint* groups, int count)
    {
        var slots = ((PrefetchRing)GCHandle.FromIntPtr((nint)context).Target)?._slots;
        if (slots == null)
            return; // without io_uring nothing waits
        for (var i = 0; i < count; i++)
        {
            try
            {
                slots[groups[i]].Complete();
            }
            catch
            {
                // an exception must not reach the native thread
            }
        }
    }

    public unsafe void Dispose()
    {
        var handle = _handle;
        if (handle == null)
            return;
        var rc = Pal.rvn_prefetch_ring_destroy(handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, "Failed to destroy the prefetch ring: data files are still open with it");
        _handle = null;
        _self.Free();
    }
}
