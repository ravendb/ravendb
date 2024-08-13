using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.Changes;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Operations;

public abstract class AbstractOperations<TOperation> : ILowMemoryHandler
    where TOperation : AbstractOperation, new()
{
    internal const long InvalidOperationId = -1;

    private readonly IDocumentsChanges _changes;
    private readonly TimeSpan _maxCompletedTaskLifeTime;

    protected readonly ConcurrentDictionary<long, AbstractOperation> Active = new();
    internal readonly ConcurrentDictionary<long, AbstractOperation> Completed = new();

    protected AbstractOperations(IDocumentsChanges changes, TimeSpan maxCompletedTaskLifeTime)
    {
        _changes = changes;
        _maxCompletedTaskLifeTime = maxCompletedTaskLifeTime;

        LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
    }

    public abstract Task<IOperationResult> AddLocalOperation(
        long id,
        OperationType operationType,
        string description,
        IOperationDetailedDescription detailedDescription,
        Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory,
        OperationCancelToken token = null);

    protected Task<IOperationResult> AddOperationInternalAsync(AbstractOperation operation, Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory)
    {
        var id = operation.Id;
        var operationDescription = operation.Description;
        var operationType = operationDescription.TaskType;
        var operationState = operation.State;

        var notification = new OperationStatusChange
        {
            OperationId = operation.Id,
            State = operationState
        };

        object locker = new();
        Monitor.Enter(locker);
        try
        {
            operation.Task = Task.Run(() => taskFactory(ProgressNotification));
            operation.Task.ContinueWith(ContinuationFunction);
            Active.TryAdd(id, operation);

            if (operation.Token == null)
                return operation.Task;

            return operation.Task.ContinueWith(t =>
            {
                operation.Token.Dispose();
                return t;
            }).Unwrap();
        }
        finally
        {
            Monitor.Exit(locker);
        }

        void ContinuationFunction(Task<IOperationResult> taskResult)
        {
            operationDescription.EndTime = SystemTime.UtcNow;
            operationState.Progress = null;

            if (taskResult.IsCanceled)
            {
                operationState.Result = null;
                operationState.Status = OperationStatus.Canceled;
            }
            else if (taskResult.IsFaulted)
            {
                var innerException = taskResult.Exception.ExtractSingleInnerException();

                var isConflict = innerException is DocumentConflictException or ConcurrencyException;
                var status = isConflict ? HttpStatusCode.Conflict : HttpStatusCode.InternalServerError;

                var shouldPersist = false;

                switch (operationType)
                {
                    case OperationType.DatabaseExport:
                    case OperationType.DatabaseImport:
                        shouldPersist = true;
                        break;
                }

                operationState.Result = new OperationExceptionResult(innerException, status, shouldPersist);
                operationState.Status = OperationStatus.Faulted;
            }
            else
            {
                operationState.Result = taskResult.Result;
                operationState.Status = OperationStatus.Completed;
            }

            if (Monitor.TryEnter(locker) == false)
            {
                // adding of operation still didn't finish, just exit
                RaiseNotifications(notification, operation);
                return;
            }

            try
            {
                if (Active.TryGetValue(id, out AbstractOperation completed))
                {
                    completed.SetCompleted();
                    // add to completed items before removing from active ones to ensure an operation status is accessible all the time
                    Completed.TryAdd(id, completed);
                    Active.TryRemove(id, out completed);
                }

                RaiseNotifications(notification, operation);
            }
            finally
            {
                Monitor.Exit(locker);
            }
        }

        void ProgressNotification(IOperationProgress progress)
        {
            notification.State.Progress = progress;
            RaiseNotifications(notification, operation);
        }
    }

    public async ValueTask KillOperationAsync(long id, CancellationToken token)
    {
        if (Completed.ContainsKey(id))
            return;

        if (Active.TryGetValue(id, out AbstractOperation operation) == false)
            throw new ArgumentException($"Operation {id} was not registered");

        await operation.KillAsync(waitForCompletion: false, token);
    }

    public AbstractOperation GetOperation(long id)
    {
        if (Active.TryGetValue(id, out AbstractOperation operation))
        {
            return operation;
        }

        if (Completed.TryGetValue(id, out operation))
        {
            return operation;
        }

        return null;
    }

    public abstract long GetNextOperationId();

    public virtual void Dispose(ExceptionAggregator exceptionAggregator)
    {
        foreach (var active in Active.Values)
        {
            exceptionAggregator.Execute(() =>
            {
                try
                {
                    if (active.Killable)
                    {
                        active.KillAsync(waitForCompletion: true, CancellationToken.None)
                            .GetAwaiter()
                            .GetResult();
                    }
                    else
                    {
                        var task = active.Task;

                        if (task == null)
                            return;

                        if (task.Status is TaskStatus.WaitingToRun)
                            return; // execution has not even started yet

                        task.Wait(TimeSpan.FromSeconds(30));
                    }
                }
                catch (Exception)
                {
                    // we explicitly don't care about this during shutdown
                }
            });
        }

        Active.Clear();
        Completed.Clear();
    }

    public IEnumerable<AbstractOperation> GetAll() => Active.Values.Union(Completed.Values);

    public ICollection<AbstractOperation> GetActive() => Active.Values;

    public bool HasActive => Active.IsEmpty == false;

    protected TOperation CreateOperationInstance(long id, string databaseName, OperationType type, string description, IOperationDetailedDescription detailedDescription, OperationCancelToken token)
    {
        var operationState = new OperationState
        {
            Status = OperationStatus.InProgress
        };

        var operationDescription = new OperationDescription
        {
            Description = description,
            TaskType = type,
            StartTime = SystemTime.UtcNow,
            DetailedDescription = detailedDescription
        };

        var operation = new TOperation
        {
            Id = id,
            Description = operationDescription,
            Token = token,
            State = operationState,
            DatabaseName = databaseName
        };

        return operation;
    }

    protected virtual void RaiseNotifications(OperationStatusChange change, AbstractOperation operation)
    {
        _changes?.RaiseNotifications(change);
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        // cleanup operations older than 1 minute only
        // Client API might still be waiting for the status
        CleanupOperationsInternal(Completed, TimeSpan.FromMinutes(1));
    }

    public void LowMemoryOver()
    {
        // nothing to do here
    }

    internal void CleanupOperations()
    {
        CleanupOperationsInternal(Completed, _maxCompletedTaskLifeTime);
    }

    private static void CleanupOperationsInternal(ConcurrentDictionary<long, AbstractOperation> operations, TimeSpan maxCompletedTaskLifeTime)
    {
        var oldestPossibleCompletedOperation = SystemTime.UtcNow - maxCompletedTaskLifeTime;

        foreach (var taskAndState in operations)
        {
            var state = taskAndState.Value;

            if (state.Description.EndTime.HasValue && state.Description.EndTime < oldestPossibleCompletedOperation)
            {
                operations.TryRemove(taskAndState.Key, out _);
            }
        }
    }
}
