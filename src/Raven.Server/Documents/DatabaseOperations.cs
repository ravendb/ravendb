using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Data;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DatabaseOperations
    {
        private readonly Logger _logger;
        private readonly ConcurrentDictionary<long, PendingOperation> _pendingOperations = new ConcurrentDictionary<long, PendingOperation>();

        private long _pendingOperationsCounter;

        public DatabaseOperations(DocumentDatabase db)
        {
            _logger = LoggingSource.Instance.GetLogger<DatabaseOperations>(db.Name);
        }

        internal void ClearCompletedPendingTasks()
        {
            foreach (var taskAndState in _pendingOperations)
            {
                var status = taskAndState.Value.State.Status;
                if (status != OperationStatus.InProgress)
                {
                    PendingOperation value;
                    _pendingOperations.TryRemove(taskAndState.Key, out value);
                }
                if (status == OperationStatus.Faulted || status == OperationStatus.Canceled)
                {
                    var exceptionResult = taskAndState.Value.State.Result as OperationExceptionResult;
                    
                    if (_logger.IsOperationsEnabled && exceptionResult != null)
                        _logger.Operations($"Failed to execute background task {taskAndState.Key} {exceptionResult.Message} {exceptionResult.StackTrace}");
                }
            }
        }

        //TODO: get all

        public OperationState GetOperationState(long id)
        {
            PendingOperation operation;
            if (_pendingOperations.TryGetValue(id, out operation))
            {
                return operation.State;
            }
            return null;
        }

        public async Task<IOperationResult> ExecuteOperation(string description, PendingOperationType operationType, JsonOperationContext context, Func<Action<IOperationProgress>, 
            Task<IOperationResult>> operation, WebSocket socket, OperationCancelToken token = null)
        {
            var tcs = new TaskCompletionSource<IOperationResult>();
            var id = GetNextOperationId();

            var operationState = new OperationState
            {
                Status = OperationStatus.InProgress
            };

            var notification = new OperationStatusChangeNotification
            {
                OperationId = id,
                State = operationState
            };
            
            var operationDescription = new PendingOperationDescription
            {
                Description = description,
                TaskType = operationType,
                StartTime = SystemTime.UtcNow
            };

            _pendingOperations.TryAdd(id, new PendingOperation
            {
                Description = operationDescription,
                Token = token,
                State = operationState,
                Task = tcs.Task
            });

            Action<IOperationProgress> action = async progress =>
            {
                notification.State.Progress = progress;
                await SendOperationStatus(context, socket, notification, token);
            };

            try
            {
                // send intial operation progress to notify about operation id
                await SendOperationStatus(context, socket, notification, token);

                var operationResult = await operation(action).ConfigureAwait(false);

                operationState.Result = operationResult;
                operationState.Status = OperationStatus.Completed;
                tcs.SetResult(operationResult);

                return operationResult;
            }
            catch (OperationCanceledException e)
            {
                operationState.Status = OperationStatus.Canceled;
                tcs.SetException(e);
                throw;
            }
            catch (Exception e)
            {
                operationState.Result = new OperationExceptionResult(e);
                operationState.Status = OperationStatus.Faulted;
                tcs.SetException(e);
                throw;
            }
            finally
            {
                operationState.Progress = null;
                operationDescription.EndTime = SystemTime.UtcNow;
                await SendOperationStatus(context, socket, notification, token);
            }
        }

        private async Task SendOperationStatus(JsonOperationContext context, WebSocket webSocket, OperationStatusChangeNotification notification, OperationCancelToken token)
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = new BlittableJsonTextWriter(context, ms))
                {
                    var notificationJson = notification.ToJson();
                    context.Write(writer, notificationJson);
                }

                ArraySegment<byte> bytes;
                ms.TryGetBuffer(out bytes);
                    
                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, token.Token);
            }
        }

        public void KillRunningOperation(long id)
        {
            PendingOperation value;
            if (_pendingOperations.TryGetValue(id, out value))
            {
                if (value.State.Status == OperationStatus.InProgress)
                {
                    value.Token?.Cancel();
                }
            }
        }

        public long GetNextOperationId()
        {
            return Interlocked.Increment(ref _pendingOperationsCounter);
        }

        public void RemoveOperation(long operationId)
        {
            PendingOperation value;
            _pendingOperations.TryRemove(operationId, out value);
        }

        public void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var pendingTaskAndState in _pendingOperations.Values)
            {
                exceptionAggregator.Execute(() =>
                {
                    try
                    {
                        pendingTaskAndState.Task.Wait();
                    }
                    catch (Exception)
                    {
                        // we explictly don't care about this during shutdown
                    }
                });
            }

            _pendingOperations.Clear();
        }

        public class PendingOperation
        {
            public PendingOperationDescription Description;
            public OperationCancelToken Token;
            public OperationState State;
            public Task Task;
        }

        public class PendingOperationDescription
        {
            public string Description;
            public PendingOperationType TaskType;
            public DateTime StartTime;
            public DateTime EndTime;
        }

        public enum PendingOperationType 
        {
            UpdateByIndex,

            DeleteByIndex
        }
    }
}