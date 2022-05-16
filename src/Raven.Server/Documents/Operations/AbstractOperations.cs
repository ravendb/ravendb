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

namespace Raven.Server.Documents.Operations;

public abstract class AbstractOperations<TOperation>
    where TOperation : AbstractOperation, new()
{
    private readonly IDocumentsChanges _changes;

    protected readonly ConcurrentDictionary<long, TOperation> Active = new();
    protected readonly ConcurrentDictionary<long, TOperation> Completed = new();

    protected AbstractOperations(IDocumentsChanges changes)
    {
        _changes = changes;
    }

    protected Task<IOperationResult> AddOperationInternalAsync(TOperation operation, Func<Action<IOperationProgress>, Task<IOperationResult>> taskFactory)
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
                if (Active.TryGetValue(id, out TOperation completed))
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

    public ValueTask KillOperationAsync(long id, CancellationToken token)
    {
        if (Active.TryGetValue(id, out TOperation operation) == false)
            throw new ArgumentException($"Operation {id} was not registered");

        if (operation.Killable == false)
            throw new ArgumentException($"Operation {id} is unkillable");

        return operation.KillAsync(waitForCompletion: false, token);
    }

    public TOperation GetOperation(long id)
    {
        if (Active.TryGetValue(id, out TOperation operation))
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

    public void Dispose(ExceptionAggregator exceptionAggregator)
    {
        foreach (var active in Active.Values)
        {
            exceptionAggregator.Execute(() =>
            {
                try
                {
                    active.KillAsync(waitForCompletion: true, CancellationToken.None)
                        .AsTask()
                        .GetAwaiter()
                        .GetResult();
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

    public IEnumerable<TOperation> GetAll() => Active.Values.Union(Completed.Values);

    public ICollection<TOperation> GetActive() => Active.Values;

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

    protected virtual void RaiseNotifications(OperationStatusChange change, TOperation operation)
    {
        _changes?.RaiseNotifications(change);
    }
}
