using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl.Journal;

/// <summary>
/// This controls the flow of writes to the journal. Trying to maximize throughput while minimizing latency. The mechanisms are:
///
///  1. Async transaction chaining (the merger)
///     - While batch N's journal write is in flight, the merger already executes batch N+1; N's write latency is N+1's collection window.
///       This is the only mechanism that GROWS batches.
///     - Good: enough load to parallelize the journal write with processing the next transactions (> 40% perf diff).
///     - Bad: not enough load - all operations merge into one batch and the system moves in peaks and valleys
///       (do a lot of work, notify the clients, wait for them to process, then issue the next requests, etc).
///
///  2. Journal write pipelining
///     - Submits write N+1 to the device while write N is still in flight, instead of serializing them behind the write lock.
///       Chaining (above) overlaps EXECUTION with a single write in flight - the writes themselves stay serial;
///       pipelining overlaps the WRITES with each other, exploiting the device's own parallelism, so the merger
///       no longer pays a full device round trip per batch.
///     - Good: batches that cannot grow anyway. At low closed-loop concurrency the population splits into two
///       groups (batch N's clients are notified only after batch N+1 closes), and overlapping the two groups'
///       writes is the only available lever.
///     - Bad: anywhere the batch could have grown - releasing the merger early shrinks the batches; and on
///       large writes we are bandwidth-bound, so more writes in flight only add queueing.
///     - How we determine: recent batches mostly closed starved (they could not have grown), the write is slow
///       enough to be worth hiding, and the writes are small (we are not bandwidth-bound). Streams with no
///       batch telemetry (indexing, raw async commits) have externally-fixed batches and are always eligible.
///
///  3. Batch consolidation
///     - Holds the current batch open past the natural window so that one large write replaces several
///       small ones, amortizing the fixed per-transaction and per-write costs.
///     - Good: many small commits against a per-write cost - an IOPS-budgeted volume, or the fixed commit
///       overhead itself at saturation.
///     - Bad: the queue empties during the hold (no new ops, waiting is pure latency); the write is already
///       large (the fixed cost is amortized, a bigger batch only adds cycle time); or absorbing the queue's 
///       last tail (the next batch then opens empty and idles a client round trip - leave that to the next tx).
///     - How we determine: engage while the typical journal write is below the target size (configuration-pinned,
///       or walked at runtime by a bounded hill-climb on measured throughput); stop the moment the queue starves;
///       extend only while the queue could at least double the batch.
///
///  4. Sync (fsync) deferral
///     - Delays the data-file sync while the unsynced backlog is modest, so writeback accumulates and merges
///       (dead writes are written once) instead of syncing eagerly against the commit traffic.
///     - Good: the sync spends the same device budget as the journal writes - fewer, larger syncs collide
///       less with user-facing commits, and merged writeback moves fewer bytes overall.
///     - Bad: the backlog eventually has to be written, higher peak I/O. Deferring too long trades durability 
///       housekeeping for I/O smoothing.
///     - How we determine: sync once enough journals or unsynced bytes pile up, unless the backlog is still
///       under the mandatory cap; past the cap the sync always runs. How the sync then behaves against the
///       shared disk (trickle vs drain) is the DeviceWriteBudget's call, not ours.
///
/// This class is the PER-ENVIRONMENT half: batch shaping, pipelining, this stream's codec and sync scheduling. 
/// Signals that are properties of the physical DISK - live in shared, per-device <see cref="DeviceWriteBudget"/>.
///
/// Telemetry inputs (owned here, fed by the components):
///  - journal write latency and size (from the write pipeline, per completed device write)
///  - why each merged batch closed, and how large it was (from the merger, per batch)
/// </summary>
public sealed class WriteFlowPolicy
{
    // discriminator between journal pipelining and batch consolidation.
    public enum BatchCloseReason
    {
        // ran out of operations and the dry-up exit fired
        // no way to grow the batch, no operations to execute
        QueueStarved, 
        // the batching window expired with work still queued, batch growth possible
        WindowElapsed,  
        // hit the transaction / consolidation size cap, batch hit max limit
        SizeReached,    
    }

    // ---------------------------------------------------------------------------------------
    // The consolidation target: how large a journal write consolidation aims for
    // ---------------------------------------------------------------------------------------
    // The measured optima depend on the workload & device used. 
    // The band's optimum is wide and flat, so coarse steps and a noise dead-band are enough;
    // the worst a wrong step costs is one window inside the band.
    private static readonly long[] TargetWriteSizeLadder =
    {
        16 * Constants.Size.Kilobyte, 24 * Constants.Size.Kilobyte, 32 * Constants.Size.Kilobyte,
        48 * Constants.Size.Kilobyte, 64 * Constants.Size.Kilobyte, 96 * Constants.Size.Kilobyte,
        128 * Constants.Size.Kilobyte, 192 * Constants.Size.Kilobyte, 256 * Constants.Size.Kilobyte
    };

    private const int SeedLadderIndex = 5; // 96KB - inside every measured optimum band

    private static readonly long EvaluationWindowTicks = Stopwatch.Frequency * 5; // wall clock per step
    private const long MinBatchesPerEvaluation = 256;   // fewer means idle - nothing to tune
    private const double EvaluationNoiseBand = 0.03;    // |change| below this is a flat reading

    private readonly long _pinnedTargetWriteSizeBytes;  // configuration; 0 = adapt at runtime
    private int _targetLadderIndex = SeedLadderIndex;
    private int _targetClimbDirection = 1;
    private long _evaluationWindowStart;
    private long _evaluationWindowOperations;
    private long _evaluationWindowBatches;
    private long _evaluationWindowConsolidatedBatches;
    private double _previousWindowOperationsPerSecond;

    private long TargetWriteSizeBytes =>
        _pinnedTargetWriteSizeBytes > 0 ? _pinnedTargetWriteSizeBytes : TargetWriteSizeLadder[_targetLadderIndex];

    private const long MostlyStarvedPerMille = 900;

    private readonly long _pipelineAboveLatencyTicks;
    private readonly int _maxConcurrentJournalWrites;

    private SimpleEwma _writeLatencyTicks = new(smoothing: 8);
    private SimpleEwma _writeSizeBytes = new(smoothing: 8);

    private volatile bool _consolidatingBatches;

    private long _batchesClosedStarved;
    private long _batchesClosedOnWindow;
    private long _batchesClosedOnSize;

    private SimpleEwma _batchModifiedBytes = new(smoothing: 16);
    private SimpleEwma _starvedClosesPerMille = new(smoothing: 16);

    private readonly StorageEnvironmentOptions _options;

    private DeviceWriteBudget _unsharedDevice;

    private DeviceWriteBudget Device =>
        _options.DeviceWriteBudget ??
        _unsharedDevice  ?? // resolved lazily because pager identifies the device after this policy is constructed
        Interlocked.CompareExchange(ref _unsharedDevice, DeviceWriteBudget.CreateUnshared(_options), null) ??
        _unsharedDevice;

    public WriteFlowPolicy(StorageEnvironmentOptions options)
    {
        _options = options;
        _pipelineAboveLatencyTicks = options.PipelineJournalWritesAboveLatencyInTicks;
        _pinnedTargetWriteSizeBytes = options.ConsolidationTargetWriteSizeInBytes;
        _maxConcurrentJournalWrites = Math.Clamp(options.MaxConcurrentJournalWrites, 1, StorageEnvironmentOptions.MaxSupportedConcurrentJournalWrites);
    }

    public void RecordJournalWrite(long latencyTicks, long sizeInBytes)
    {
        _writeLatencyTicks.Update(latencyTicks);
        _writeSizeBytes.Update(sizeInBytes);
        Device.RecordJournalWrite(latencyTicks, sizeInBytes);
    }

    public void RecordJournalWriteSubmitted() => Device.RecordJournalWriteActivity();

    public void RecordBatchClosed(BatchCloseReason reason, int operations, long modifiedBytes)
    {
        if (operations == 0)
            return; // chain unwinding, an empty batch says nothing about batch shaping

        switch (reason)
        {
            case BatchCloseReason.QueueStarved: _batchesClosedStarved++; break;
            case BatchCloseReason.WindowElapsed: _batchesClosedOnWindow++; break;
            case BatchCloseReason.SizeReached: _batchesClosedOnSize++; break;
        }

        _batchModifiedBytes.Update(modifiedBytes);
        _starvedClosesPerMille.Update(reason == BatchCloseReason.QueueStarved ? 1000 : 0);

        _evaluationWindowBatches++;
        _evaluationWindowOperations += operations;
        if (_consolidatingBatches)
            _evaluationWindowConsolidatedBatches++;
        MaybeStepConsolidationTarget();
    }

    private void MaybeStepConsolidationTarget()
    {
        if (_pinnedTargetWriteSizeBytes > 0)
            return;

        var now = Stopwatch.GetTimestamp();
        if (_evaluationWindowStart == 0)
        {
            _evaluationWindowStart = now;
            return;
        }

        var elapsed = now - _evaluationWindowStart;
        if (elapsed < EvaluationWindowTicks || _evaluationWindowBatches < MinBatchesPerEvaluation)
        {
            // a window that dragged far past its length spans an idle gap - start over
            if (elapsed > 4 * EvaluationWindowTicks)
            {
                _previousWindowOperationsPerSecond = 0;
                _evaluationWindowStart = now;
                _evaluationWindowOperations = 0;
                _evaluationWindowBatches = 0;
                _evaluationWindowConsolidatedBatches = 0;
            }

            return;
        }

        var operationsPerSecond = (double)_evaluationWindowOperations * Stopwatch.Frequency / elapsed;

        // let consolidation shape majority of the batches before action, otherwise, it's just noise
        var steering = _evaluationWindowConsolidatedBatches * 2 > _evaluationWindowBatches;
        if (steering && _previousWindowOperationsPerSecond > 0)
        {
            var change = (operationsPerSecond - _previousWindowOperationsPerSecond) / _previousWindowOperationsPerSecond;
            if (change < -EvaluationNoiseBand)
                _targetClimbDirection = -_targetClimbDirection; // that step hurt - walk it back

            if (Math.Abs(change) > EvaluationNoiseBand)
            {
                var next = _targetLadderIndex + _targetClimbDirection;
                if (next < 0 || next >= TargetWriteSizeLadder.Length)
                {
                    _targetClimbDirection = -_targetClimbDirection;
                    next = _targetLadderIndex + _targetClimbDirection;
                }

                _targetLadderIndex = next;
            }
            // a flat reading holds the position: the optimum is a wide plateau, sitting on it is the goal
        }

        _previousWindowOperationsPerSecond = steering ? operationsPerSecond : 0; // a non-steering window is no baseline
        _evaluationWindowStart = now;
        _evaluationWindowOperations = 0;
        _evaluationWindowBatches = 0;
        _evaluationWindowConsolidatedBatches = 0;
    }

    // if we are making large writes, we'll be limited by device bandwidth, not latency.
    private bool IsCommitLatencyBound => _writeSizeBytes.Current < 256 * Constants.Size.Kilobyte;

    private bool IsMeasuredFastDevice => Device.IsMeasuredFastDevice;

    private bool PipeliningEnabled => _maxConcurrentJournalWrites > 1;

    private bool ShouldPipeline =>
        PipeliningEnabled &&
        IsCommitLatencyBound && // not meaningful if we are bandwidth-bound
        (HasBatchTelemetry == false || // no batching == cannot grow the batch to amortize fixed costs, pipelining is always a win
         _starvedClosesPerMille.Current >= MostlyStarvedPerMille) && // most recent batches closed starved, they cannot grow
        // the device is slow enough that overlapping writes pays for the smaller batches
        _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks;

    public bool CanPipeline(long totalNumberOf4Kbs) =>
        ShouldPipeline &&
        // < 1MB, otherwise memcpy + large write, etc. Doesn't pay off.
        totalNumberOf4Kbs <= JournalWritePipeline.MaxPipelinedBatch4Kbs;

    private bool HasBatchTelemetry => Volatile.Read(ref _batchesClosedStarved) + Volatile.Read(ref _batchesClosedOnWindow) + Volatile.Read(ref _batchesClosedOnSize) > 0;

    // how many async-committed transactions the merger leaves in flight instead of completing
    public int KeepInFlightJournalWrites => ShouldPipeline ? _maxConcurrentJournalWrites - 1 : 0;

    private const long MaxBatchConsolidationWindowInMs = 50;

    private const long MaxBatchConsolidationSizeInBytes = 128 * Constants.Size.Megabyte;

    private const int MaxConsecutiveEmptyConsolidationWaits = 2; // > 2 ms wait max, avoid high latency / stalls

    public bool ConsolidatingBatches => _consolidatingBatches;

    public double GetBatchingWindowDurationInMs(double configuredMinimumMs)
    {
        var writeSize = _writeSizeBytes.Current;

        // if typical write is under target size == not yet amortizing fixed costs
        _consolidatingBatches = writeSize > 0 && writeSize < TargetWriteSizeBytes;

        // Extend to window cap during consolidation; fallback to base floor otherwise
        return _consolidatingBatches
            ? Math.Max(configuredMinimumMs, MaxBatchConsolidationWindowInMs)
            : configuredMinimumMs;
    }
    public long ConsolidationSizeLimitInBytes
    {
        get
        {
            var writeSize = _writeSizeBytes.Current;
            var modified = _batchModifiedBytes.Current;

            if (writeSize <= 0 || modified <= 0) // no good data yet, safe defaults
                return MaxBatchConsolidationSizeInBytes;

            // Convert target write size to actual modified bytes (accounts for diffing + compression)
            var modifiedBytesPerWrittenByte = (double)modified / writeSize;
            var limit = (long)(TargetWriteSizeBytes * Math.Max(1, modifiedBytesPerWrittenByte));

            // Clamp limit between target floor and max allowed batch cap
            return Math.Clamp(limit, TargetWriteSizeBytes, MaxBatchConsolidationSizeInBytes);
        }
    }

    // Leave a tail in the queue when consolidating to seed the next batch immediately. Draining those 
    // last few ops means all clients have to be notified, instead of staggering the work to increase throughput
    public int MinQueueDepthToKeepAbsorbing => _consolidatingBatches ? 8 : 0;

    // batches so small they aren't worth considering in our calculations
    public const int TinyBatchOperations = 8;

    // How long to humor an empty queue before closing the batch. An empty queue means one of
    // two things, and the recent starved share tells them apart:
    //  - almost every batch closes starved: the population is arrival-capped, waiting won't help.
    //  - closes are mixed: the previous batch's clients will come back to us shortly, waiting improve throughput
    public int EmptyConsolidationWaitLimit =>
        _starvedClosesPerMille.Current >= MostlyStarvedPerMille
            ? 0
            : MaxConsecutiveEmptyConsolidationWaits;

   
    // On NVMe devices, writing the full data to disk is _faster_ than compressing it first (CPU bound, not I/O bound).
    private const int FastDeviceCompressTxAboveSizeInBytes = 512 * Constants.Size.Kilobyte;

    public long GetCompressTxAboveSizeInBytes(long configured) =>
        IsMeasuredFastDevice ? Math.Max(configured, FastDeviceCompressTxAboveSizeInBytes) : configured;

    public JournalCompressionAlgorithm ResolveJournalCompressionAlgorithm(JournalCompressionAlgorithm configured)
    {
        if (configured != JournalCompressionAlgorithm.Auto)
            return configured; // pinned by the user, in either direction

        // Zstd is 400MB/sec vs. LZ4 1.5GB/sec - only make sense to go to this effort if the device is constrained
        return Device.MeasuredDeviceClass == DeviceWriteBudget.DeviceClass.Budgeted
            ? JournalCompressionAlgorithm.Zstd
            : JournalCompressionAlgorithm.Lz4;
    }

    // journal writes are what a user waits for, they have highest priority. 
    // We can't starve the flusher indefinitely either, and high backlog would inflate journal write latency.
    // This is the balance between the two, defer flushing when we can, but not too long to have I/O storm
    public bool ShouldFlusherYieldToJournal(long unflushedPages) =>
        _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks * 2 &&
        unflushedPages < 4 * _options.MaxNumberOfPagesInJournalBeforeFlush;

    // The sync is a shared device operation, it competes with every other write on the disk.
    // Callers force past this for explicit and required syncs.
    public bool ShouldSyncNow(StorageEnvironment env)
    {
        var journalsPendingSync = env.Journal.Files.Count + env.Journal.Applicator.JournalsToDeleteCount;

        if (journalsPendingSync > _options.SyncJournalsCountThreshold)
            return true; // journal reuse is stalling behind the sync

        var totalWrittenButUnsyncedBytes = env.Journal.Applicator.TotalWrittenButUnsyncedBytes;

        if (totalWrittenButUnsyncedBytes <= _options.MaxUnsyncedBytesBeforeSync)
            return false; // not enough accumulated to be worth a shared device operation

        // past the soft threshold we may still defer, letting the paced writeback trickle to the disk

        if (_options.SyncWritebackBlockSizeInMb <= 0 || _options.RunningOn32Bits)
            return true; // but not if this is disabled, of course

        // too much not sycned, we want to avoid I/O storm, so we force sync (hopefully previous writebacks helped)
        return totalWrittenButUnsyncedBytes > _options.MaxUnsyncedBytesBeforeMandatorySync;
    }

    public bool ShouldPrepareZeroedJournalsInBackground => Device.ShouldPrepareZeroedJournalsInBackground;

    public int NextJournalZeroingStepMs(bool journalWriteActive, int stalledSoFarMs) =>
        Device.NextJournalZeroingStepMs(journalWriteActive, stalledSoFarMs);
}
