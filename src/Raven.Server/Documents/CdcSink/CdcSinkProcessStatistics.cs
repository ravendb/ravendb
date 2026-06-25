using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Documents.ETL;

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

    public CdcSinkProcessStatistics(string processName)
    {
        _processName = processName;
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
            // re-executed command stores only the errors from its final attempt.
            _itemErrors.Clear();
        }
    }
}
