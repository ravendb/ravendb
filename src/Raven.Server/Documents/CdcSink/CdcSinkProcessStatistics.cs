using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;
using Raven.Server.Utils.Metrics;

namespace Raven.Server.Documents.CdcSink;

public class CdcSinkProcessStatistics
{
    private readonly string _processName;

    // Mutated from both the process thread (RecordConsumeError) and the TxMerger thread
    // (ConsumeSuccess / RecordPartialConsumeError / RecordScriptExecutionError / NewBatch). All
    // mutations take this lock so the counters' compound threshold checks stay atomic and the
    // in-memory error buffer isn't corrupted by concurrent Add/Clear. Cross-thread reads of the
    // int/bool counters for monitoring are intentionally lock-free (atomic reads; a slightly stale
    // value is acceptable there).
    private readonly object _lock = new();

    // Per-batch item errors (per-document apply failures + JS-script failures). Buffered here while
    // the batch executes inside the TxMerger and flushed to TaskErrorsStorage from the process
    // thread once the batch completes (see CdcSinkProcess.SubmitBatch), mirroring EtlProcessStatistics.
    private readonly List<TaskItemError> _itemErrors = new();

    private readonly float _healthFailedThreshold;
    private readonly float _healthImpairedThreshold;

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

    public CdcSinkProcessStatistics(string processName, float healthFailedThreshold, float healthImpairedThreshold)
    {
        _processName = processName;
        _healthFailedThreshold = healthFailedThreshold;
        _healthImpairedThreshold = healthImpairedThreshold;
    }

    public int ConsumeSuccesses { get; private set; }

    public int ConsumeErrors { get; set; }

    public DateTime? LastConsumeErrorTime { get; private set; }

    public bool WasLatestConsumeSuccessful { get; set; }

    private int ScriptExecutionErrors { get; set; }

    public void ConsumeSuccess(int items)
    {
        lock (_lock)
        {
            WasLatestConsumeSuccessful = true;
            ConsumeSuccesses += items;
            _batchSuccesses += items;
        }
    }

    public void RecordConsumeError(string error, int count = 1)
    {
        lock (_lock)
        {
            WasLatestConsumeSuccessful = false;

            ConsumeErrors += count;

            LastConsumeErrorTime = SystemTime.UtcNow;

            if (ConsumeErrors <= ConsumeSuccesses)
                return;

            var message = $"Consume error ratio is too high (errors: {ConsumeErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate consume error ratio and stopped current CDC Sink batch.";

            throw new InvalidOperationException($"{message}. Current stats: {this}. Error: {error}");
        }
    }

    public void RecordScriptExecutionError(Exception e, string documentId)
    {
        lock (_lock)
        {
            ScriptExecutionErrors++;

            _itemErrors.Add(new TaskItemError
            {
                CreatedAt = SystemTime.UtcNow,
                TaskName = _processName,
                DocumentId = documentId,
                Step = TaskErrorStep.Transformation,
                Error = e.ToString()
            });

            if (ScriptExecutionErrors < 100)
                return;

            if (ScriptExecutionErrors <= ConsumeSuccesses)
                return;

            var message = $"Script execution error ratio is too high (errors: {ScriptExecutionErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate script execution error ratio and stopped current batch.";

            throw new InvalidOperationException($"{message}. Current stats: {this}. Error: {e}");
        }
    }

    /// <summary>
    /// Records a partial consume error for a single document group that failed processing.
    /// Uses the same threshold logic as ETL's RecordPartialLoadError:
    /// tolerate errors while under 100 cumulative errors OR while errors &lt;= successes.
    /// When both thresholds are exceeded, throws to prevent LSN advancement.
    /// </summary>
    public void RecordPartialConsumeError(string error, string documentId)
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
                Step = TaskErrorStep.Load,
                Error = error
            });

            LastConsumeErrorTime = SystemTime.UtcNow;

            if (ConsumeErrors < 100)
                return;

            if (ConsumeErrors <= ConsumeSuccesses)
                return;

            var message = $"Consume error ratio is too high (errors: {ConsumeErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate consume error ratio and stopped current CDC Sink batch.";

            throw new InvalidOperationException($"{message}. Current stats: {this}. Document: '{documentId}'. Error: {error}");
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
            // error/success tally is reset for the same reason — only the final attempt feeds the EWMA.
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

            var errorsRatio = AverageErrorsRatio.GetRate();
            HealthStatus = errorsRatio switch
            {
                _ when errorsRatio > _healthFailedThreshold => EtlProcessHealthStatus.Failed,
                _ when errorsRatio > _healthImpairedThreshold => EtlProcessHealthStatus.Impaired,
                _ => EtlProcessHealthStatus.Healthy
            };

            _batchErrors = 0;
            _batchSuccesses = 0;
        }
    }

    /// <summary>
    /// Forces <see cref="HealthStatus"/> to <see cref="EtlProcessHealthStatus.Failed"/> when the process
    /// hits a permanent configuration/schema fault and stops retrying — no further batch completes to move
    /// the EWMA, so the health would otherwise stay stale. Mirrors ETL's script-parse-error health override.
    /// </summary>
    public void SetHealthStatusToFailed()
    {
        lock (_lock)
        {
            HealthStatus = EtlProcessHealthStatus.Failed;
        }
    }
}
