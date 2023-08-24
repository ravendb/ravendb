using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.Documents.QueueSink;

public class QueueSinkProcessStatistics
{
    private readonly string _processTag;
    private readonly string _processName;
    private readonly DatabaseNotificationCenter _notificationCenter;

    public QueueSinkProcessStatistics(string processTag, string processName, DatabaseNotificationCenter notificationCenter)
    {
        _processTag = processTag;
        _processName = processName;
        _notificationCenter = notificationCenter;
    }

    public int ConsumeSuccesses { get; private set; }
    
    public int ConsumeErrors { get; set; }
    
    public DateTime? LastConsumeErrorTime { get; private set; }
    
    public Queue<QueueSinkErrorInfo> ConsumeErrorsInCurrentBatch { get; } = new();
    
    public Queue<QueueSinkErrorInfo> ScriptExecutionErrorsInCurrentBatch { get; } = new();
    
    public bool WasLatestConsumeSuccessful { get; set; }
    
    public AlertRaised LastAlert { get; set; }

    private int ScriptExecutionErrors { get; set; }
    
    public void ConsumeSuccess(int items)
    {
        WasLatestConsumeSuccessful = true;
        ConsumeSuccesses += items;
    }
    
    public void RecordConsumeError(string error, int count = 1)
    {
        WasLatestConsumeSuccessful = false;

        ConsumeErrors += count;

        ConsumeErrorsInCurrentBatch.Enqueue(new QueueSinkErrorInfo(error));

        LastConsumeErrorTime = SystemTime.UtcNow;

        if (ConsumeErrors <= ConsumeSuccesses)
            return;

        var message = $"Consume error ratio is too high (errors: {ConsumeErrors}, successes: {ConsumeSuccesses}). " +
                      "Could not tolerate consume error ratio and stopped current Queue Sink batch.";

        CreateAlertIfAnyConsumeErrors(message);

        throw new InvalidOperationException($"{message}. Current stats: {this}. Error: {error}");
    }
    
    public void RecordScriptExecutionError(Exception e)
    {
        ScriptExecutionErrors++;

        ScriptExecutionErrorsInCurrentBatch.Enqueue(new QueueSinkErrorInfo(e.ToString()));

        if (ScriptExecutionErrors < 100)
            return;

        var message = $"Script execution error ratio is too high (errors: {ScriptExecutionErrors}). " +
                      "Could not tolerate script execution error ratio and stopped current batch. ";
        
        CreateAlertIfAnyScriptExecutionErrors(message);

        throw new InvalidOperationException($"{message}. Current stats: {this}");
    }
    
    private void CreateAlertIfAnyConsumeErrors(string preMessage = null)
    {
        if (ConsumeErrorsInCurrentBatch.Count == 0)
            return;
        
        LastAlert = _notificationCenter.QueueSinkNotifications.AddConsumeErrors(_processTag, _processName, ConsumeErrorsInCurrentBatch, preMessage);

        ConsumeErrorsInCurrentBatch.Clear();
    }
    
    private void CreateAlertIfAnyScriptExecutionErrors(string preMessage = null)
    {
        if (ScriptExecutionErrorsInCurrentBatch.Count == 0)
            return;

        LastAlert = _notificationCenter.QueueSinkNotifications.AddScriptErrors(_processTag, _processName, ScriptExecutionErrorsInCurrentBatch, preMessage);

        ScriptExecutionErrorsInCurrentBatch.Clear();
    }

    public IDisposable NewBatch()
    {
        ConsumeErrorsInCurrentBatch.Clear();
        ScriptExecutionErrorsInCurrentBatch.Clear();

        return new DisposableAction(() =>
        {
            CreateAlertIfAnyConsumeErrors();
            CreateAlertIfAnyScriptExecutionErrors();
        });
    }
}
