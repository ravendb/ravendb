using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils.Metrics;

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
    // thread once the batch completes (see CdcSinkProcess.SubmitBatch), mirroring EtlProcessStatistics.
    private readonly List<TaskItemError> _itemErrors = new();

    private readonly CdcSinkConfiguration _configuration;

    // Latched on a permanent fault so HealthStatus stays Failed even if a later batch completes; cleared
    // only by recreating the process (and thus these statistics).
    private bool _healthFailedLatched;

    // Per-batch error/success tally feeding the EWMA error ratio on batch completion (see OnBatchCompletion).
    private long _batchErrors;
    private long _batchSuccesses;

    /// <summary>EWMA of the per-batch error ratio, mirroring EtlProcessStatistics.AverageErrorsRatio.</summary>
    public TimeAgnosticEwma AverageErrorsRatio { get; } = new();

    /// <summary>
    /// Health derived from <see cref="AverageErrorsRatio"/> vs the configured thresholds, recomputed each
    /// batch (see <see cref="OnBatchCompletion"/>). Read lock-free by monitoring; a slightly stale value is fine.
    /// </summary>
    public EtlProcessHealthStatus HealthStatus { get; private set; } = EtlProcessHealthStatus.Healthy;

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
        }
    }

    /// <summary>
    /// Records a single document's processing failure (transformation/script or load), buffering it for
    /// flush to TaskErrorsStorage and feeding the per-batch error tally that drives the health EWMA.
    /// When this batch's error ratio gets too high it throws to fail the batch and prevent checkpoint/LSN
    /// advancement past the failed rows. The ratio is per-batch (not lifetime) so a long-healthy process's
    /// history can't mask a poisoned batch, nor can old errors trip an otherwise-healthy one.
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

            if (_batchErrors < 100)
                return;

            if (_batchErrors <= _batchSuccesses)
                return;

            var message = $"Error ratio is too high (batch errors: {_batchErrors}, batch successes: {_batchSuccesses}). " +
                          "Could not tolerate the error ratio and stopped the current CDC Sink batch.";

            throw new InvalidOperationException($"{message}. Document: '{documentId}'. Error: {error}");
        }
    }

    public int InMemoryItemErrorsCount
    {
        get
        {
            lock (_lock)
            {
                return _itemErrors.Count;
            }
        }
    }

    public List<TaskItemError> ReadInMemoryItemErrors()
    {
        lock (_lock)
        {
            return _itemErrors.ToList();
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
    /// <see cref="HealthStatus"/> from the EWMA error ratio vs the configured thresholds, mirroring
    /// EtlProcessStatistics.OnBatchCompletion. Called on the process thread after a batch is written.
    /// </summary>
    public void OnBatchCompletion()
    {
        lock (_lock)
        {
            AverageErrorsRatio.UpdateOnBatchCompletion(_batchErrors, _batchErrors + _batchSuccesses);

            if (_healthFailedLatched)
            {
                HealthStatus = EtlProcessHealthStatus.Failed;
            }
            else
            {
                var errorsRatio = AverageErrorsRatio.GetRate();
                HealthStatus = errorsRatio switch
                {
                    _ when errorsRatio > _configuration.ProcessHealthStatusFailedThreshold => EtlProcessHealthStatus.Failed,
                    _ when errorsRatio > _configuration.ProcessHealthStatusImpairedThreshold => EtlProcessHealthStatus.Impaired,
                    _ => EtlProcessHealthStatus.Healthy
                };
            }

            _batchErrors = 0;
            _batchSuccesses = 0;
        }
    }

    /// <summary>
    /// Forces <see cref="HealthStatus"/> to <see cref="EtlProcessHealthStatus.Failed"/> when the process
    /// hits a permanent configuration/schema fault and stops retrying - no further batch completes to move
    /// the EWMA, so the health would otherwise stay stale.
    /// </summary>
    public void SetHealthStatusToFailed()
    {
        lock (_lock)
        {
            _healthFailedLatched = true;
            HealthStatus = EtlProcessHealthStatus.Failed;
        }
    }
}
