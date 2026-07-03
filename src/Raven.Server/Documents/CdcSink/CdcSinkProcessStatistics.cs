using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Utils.Metrics;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.CdcSink;

public class CdcSinkProcessStatistics
{
    private readonly string _processName;

    // Mutated from both the process thread (RecordConsumeError / OnBatchCompletion / SetHealthStatusToFailed)
    // and the TxMerger thread (ConsumeSuccess / RecordItemError / NewBatch). All mutations take this lock so
    // the per-batch error/success tally and its threshold check stay atomic and the in-memory error buffer
    // isn't corrupted by concurrent Add/Clear. Cross-thread reads of the int/bool counters for monitoring
    // are intentionally lock-free (atomic reads; a slightly stale value is acceptable there).
    private readonly object _lock = new();

    // Per-batch item errors (per-document apply failures + JS-script failures). Buffered here while
    // the batch executes inside the TxMerger and flushed to TaskErrorsStorage from the process
    // thread once the batch completes (see CdcSinkProcess.SubmitBatch).
    private readonly List<TaskItemError> _itemErrors = new();

    private readonly CdcSinkConfiguration _configuration;

    // Set on a permanent fault so HealthStatus stays Failed even if a later batch completes; cleared
    // only by recreating the process (and thus these statistics).
    private bool _setHealthStatusToFailedOnFault;

    // Per-batch error/success tally feeding the EWMA error ratio on batch completion (see OnBatchCompletion).
    private long _batchErrors;
    private long _batchSuccesses;

    /// <summary>EWMA of the per-batch error ratio.</summary>
    public TimeAgnosticEwma AverageErrorsRatio { get; } = new();

    /// <summary>
    /// Health derived from <see cref="AverageErrorsRatio"/> vs the configured thresholds, recomputed each
    /// batch (see <see cref="OnBatchCompletion"/>). Read lock-free by monitoring; a slightly stale value is fine.
    /// </summary>
    public OngoingTaskHealthStatus HealthStatus { get; private set; } = OngoingTaskHealthStatus.Healthy;

    public CdcSinkProcessStatistics(string processName, CdcSinkConfiguration configuration)
    {
        _processName = processName;
        _configuration = configuration;
    }

    public int ConsumeSuccesses { get; private set; }

    public int ConsumeErrors { get; private set; }

    public DateTime? LastConsumeErrorTime { get; private set; }

    public bool WasLatestConsumeSuccessful { get; set; }

    public void ConsumeSuccess(int items)
    {
        lock (_lock)
        {
            WasLatestConsumeSuccessful = true;
            ConsumeSuccesses += items;
            _batchSuccesses += items;
        }
    }

    public void RecordConsumeError(int count = 1)
    {
        lock (_lock)
        {
            WasLatestConsumeSuccessful = false;
            ConsumeErrors += count;
            LastConsumeErrorTime = SystemTime.UtcNow;

            _batchErrors += count;
            UpdateHealthStatusOnBatchCompletion();
        }
    }

    /// <summary>
    /// Records a single document's processing failure: buffers it for flush to TaskErrorsStorage and feeds
    /// the per-batch error tally that drives the health EWMA.
    /// </summary>
    public void RecordItemError(TaskErrorStep step, string error, string documentId)
    {
        lock (_lock)
        {
            WasLatestConsumeSuccessful = false;

            ConsumeErrors++;
            _batchErrors++;

            _itemErrors.Add(new TaskItemError
            {
                CreatedAt = SystemTime.UtcNow,
                TaskName = _processName,
                DocumentId = documentId,
                Step = step,
                Error = error
            });

            LastConsumeErrorTime = SystemTime.UtcNow;
        }
    }

    /// <summary>
    /// Returns the buffered item errors and clears the buffer in one locked step. Draining on read (rather
    /// than relying on the next <see cref="NewBatch"/> to clear) prevents a re-flush of the same errors if a
    /// following batch is enqueued but never reaches <see cref="NewBatch"/> - e.g. the TxMerger rejects it on
    /// shutdown - which would otherwise duplicate the previous batch's rows in TaskErrorsStorage.
    /// </summary>
    public List<TaskItemError> DrainInMemoryItemErrors()
    {
        lock (_lock)
        {
            if (_itemErrors.Count == 0)
                return null;

            var errors = _itemErrors.ToList();
            _itemErrors.Clear();
            return errors;
        }
    }

    public void NewBatch()
    {
        lock (_lock)
        {
            // Start each batch (and each TxMerger re-run of a batch) with an empty buffer so a
            // re-executed command stores only the errors from its final attempt. The per-batch
            // error/success tally is reset for the same reason - only the final attempt feeds the EWMA.
            _itemErrors.Clear();
            _batchErrors = 0;
            _batchSuccesses = 0;
        }
    }

    /// <summary>
    /// Feeds the per-batch error/success tally into <see cref="AverageErrorsRatio"/> and recomputes
    /// <see cref="HealthStatus"/> from the EWMA error ratio vs the configured thresholds. Called on the
    /// process thread after a batch is written.
    /// </summary>
    public void OnBatchCompletion()
    {
        lock (_lock)
        {
            UpdateHealthStatusOnBatchCompletion();
        }
    }

    // Feeds the current per-batch tally into the EWMA, recomputes HealthStatus from the error ratio vs
    // the configured thresholds, then resets the tally. Caller must hold _lock.
    private void UpdateHealthStatusOnBatchCompletion()
    {
        AverageErrorsRatio.UpdateOnBatchCompletion(_batchErrors, _batchErrors + _batchSuccesses);

        if (_setHealthStatusToFailedOnFault)
        {
            HealthStatus = OngoingTaskHealthStatus.Failed;
        }
        else
        {
            HealthStatus = OngoingTaskHealthStatusExtensions.FromErrorRatio(
                AverageErrorsRatio.GetRate(),
                _configuration.ProcessHealthStatusFailedThreshold,
                _configuration.ProcessHealthStatusImpairedThreshold);
        }

        _batchErrors = 0;
        _batchSuccesses = 0;
    }

    /// <summary>
    /// Forces <see cref="HealthStatus"/> to <see cref="OngoingTaskHealthStatus.Failed"/> when the process
    /// hits a permanent configuration/schema fault and stops retrying - no further batch completes to move
    /// the EWMA, so the health would otherwise stay stale.
    /// </summary>
    public void SetHealthStatusToFailed()
    {
        lock (_lock)
        {
            _setHealthStatusToFailedOnFault = true;
            HealthStatus = OngoingTaskHealthStatus.Failed;
        }
    }
}
