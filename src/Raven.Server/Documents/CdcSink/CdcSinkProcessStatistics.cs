using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.CdcSink;

public class CdcSinkProcessStatistics
{
    private readonly string _processTag;
    private readonly string _processName;
    private readonly AbstractDatabaseNotificationCenter _notificationCenter;

    // Mutated from both the process thread (RecordConsumeError) and the TxMerger thread
    // (ConsumeSuccess / RecordPartialConsumeError / NewBatch). All mutations take this lock so the
    // counters' compound threshold checks stay atomic and the error queues aren't corrupted by
    // concurrent Enqueue/Clear. Cross-thread reads of the int/bool counters for monitoring are
    // intentionally lock-free (atomic reads; a slightly stale value is acceptable there).
    private readonly object _lock = new();

    public CdcSinkProcessStatistics(string processTag, string processName, AbstractDatabaseNotificationCenter notificationCenter)
    {
        _processTag = processTag;
        _processName = processName;
        _notificationCenter = notificationCenter;
    }

    public int ConsumeSuccesses { get; private set; }

    public int ConsumeErrors { get; set; }

    public DateTime? LastConsumeErrorTime { get; private set; }

    public Queue<CdcSinkErrorInfo> ConsumeErrorsInCurrentBatch { get; } = new();

    public Queue<CdcSinkErrorInfo> ScriptExecutionErrorsInCurrentBatch { get; } = new();

    public bool WasLatestConsumeSuccessful { get; set; }

    public AlertRaised LastAlert { get; set; }

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

            ConsumeErrorsInCurrentBatch.Enqueue(new CdcSinkErrorInfo(error));

            LastConsumeErrorTime = SystemTime.UtcNow;

            if (ConsumeErrors <= ConsumeSuccesses)
                return;

            var message = $"Consume error ratio is too high (errors: {ConsumeErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate consume error ratio and stopped current CDC Sink batch.";

            CreateAlertIfAnyConsumeErrors(message);

            throw new InvalidOperationException($"{message}. Current stats: {this}. Error: {error}");
        }
    }

    public void RecordScriptExecutionError(Exception e)
    {
        lock (_lock)
        {
            ScriptExecutionErrors++;

            ScriptExecutionErrorsInCurrentBatch.Enqueue(new CdcSinkErrorInfo(e.ToString()));

            if (ScriptExecutionErrors < 100)
                return;

            if (ScriptExecutionErrors <= ConsumeSuccesses)
                return;

            var message = $"Script execution error ratio is too high (errors: {ScriptExecutionErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate script execution error ratio and stopped current batch.";

            CreateAlertIfAnyScriptExecutionErrors(message);

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

            ConsumeErrorsInCurrentBatch.Enqueue(new CdcSinkErrorInfo($"Document '{documentId}': {error}"));

            LastConsumeErrorTime = SystemTime.UtcNow;

            if (ConsumeErrors < 100)
                return;

            if (ConsumeErrors <= ConsumeSuccesses)
                return;

            var message = $"Consume error ratio is too high (errors: {ConsumeErrors}, successes: {ConsumeSuccesses}). " +
                          "Could not tolerate consume error ratio and stopped current CDC Sink batch.";

            CreateAlertIfAnyConsumeErrors(message);

            throw new InvalidOperationException($"{message}. Current stats: {this}. Document: '{documentId}'. Error: {error}");
        }
    }

    private void CreateAlertIfAnyConsumeErrors(string preMessage = null)
    {
        if (ConsumeErrorsInCurrentBatch.Count == 0)
            return;

        LastAlert = _notificationCenter.CdcSinkNotifications.AddConsumeErrors(_processTag, _processName, ConsumeErrorsInCurrentBatch, preMessage);

        ConsumeErrorsInCurrentBatch.Clear();
    }

    private void CreateAlertIfAnyScriptExecutionErrors(string preMessage = null)
    {
        if (ScriptExecutionErrorsInCurrentBatch.Count == 0)
            return;

        LastAlert = _notificationCenter.CdcSinkNotifications.AddScriptErrors(_processTag, _processName, ScriptExecutionErrorsInCurrentBatch, preMessage);

        ScriptExecutionErrorsInCurrentBatch.Clear();
    }

    public IDisposable NewBatch()
    {
        lock (_lock)
        {
            ConsumeErrorsInCurrentBatch.Clear();
            ScriptExecutionErrorsInCurrentBatch.Clear();
        }

        return new DisposableAction(() =>
        {
            lock (_lock)
            {
                CreateAlertIfAnyConsumeErrors();
                CreateAlertIfAnyScriptExecutionErrors();
            }
        });
    }
}
