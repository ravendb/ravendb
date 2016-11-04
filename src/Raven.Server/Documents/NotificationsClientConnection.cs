using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Server.Web.Operations;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{

    public class NotificationsClientConnection : IDisposable
    {
        private static long _counter;

        private readonly WebSocket _webSocket;
        private readonly DocumentDatabase _documentDatabase;
        private readonly AsyncQueue<DynamicJsonValue> _sendQueue = new AsyncQueue<DynamicJsonValue>();
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly DateTime _startedAt;

        private readonly ConcurrentSet<string> _matchingIndexes =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocuments =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentPrefixes =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsInCollection =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsOfType =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<long> _matchingOperations =
          new ConcurrentSet<long>();

        private int _watchAllDocuments;
        private int _watchAllOperations;
        private int _watchAllAlerts;

        public NotificationsClientConnection(WebSocket webSocket, DocumentDatabase documentDatabase)
        {
            _webSocket = webSocket;
            _documentDatabase = documentDatabase;
            _startedAt = SystemTime.UtcNow;
        }

        public long Id = Interlocked.Increment(ref _counter);

        public TimeSpan Age => SystemTime.UtcNow - _startedAt;

        public void WatchDocument(string docId)
        {
            _matchingDocuments.TryAdd(docId);
        }

        public void UnwatchDocument(string name)
        {
            _matchingDocuments.TryRemove(name);
        }

        public void WatchAllDocuments()
        {
            Interlocked.Increment(ref _watchAllDocuments);
        }

        public void UnwatchAllDocuments()
        {
            Interlocked.Decrement(ref _watchAllDocuments);
        }

        public void WatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryAdd(name);
        }

        public void UnwatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryRemove(name);
        }

        public void WatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryAdd(name);
        }

        public void UnwatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryRemove(name);
        }

        public void WatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryAdd(name);
        }

        public void UnwatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryRemove(name);
        }

        public void SendDocumentChanges(DocumentChangeNotification notification)
        {
            if (_watchAllDocuments > 0)
            {
                Send(notification);
                return;
            }

            if (notification.Key != null && _matchingDocuments.Contains(notification.Key))
            {
                Send(notification);
                return;
            }

            var hasPrefix = notification.Key != null && _matchingDocumentPrefixes
                .Any(x => notification.Key.StartsWith(x, StringComparison.OrdinalIgnoreCase));
            if (hasPrefix)
            {
                Send(notification);
                return;
            }

            var hasCollection = notification.CollectionName != null && _matchingDocumentsInCollection
                .Any(x => string.Equals(x, notification.CollectionName, StringComparison.OrdinalIgnoreCase));
            if (hasCollection)
            {
                Send(notification);
                return;
            }

            var hasType = notification.TypeName != null && _matchingDocumentsOfType
                .Any(x => string.Equals(x, notification.TypeName, StringComparison.OrdinalIgnoreCase));
            if (hasType)
            {
                Send(notification);
                return;
            }

            if (notification.Key == null && notification.CollectionName == null && notification.TypeName == null)
            {
                Send(notification);
            }
        }

        private void Send(DocumentChangeNotification notification)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = "DocumentChangeNotification",
                ["Value"] = new DynamicJsonValue
                {
                    [nameof(DocumentChangeNotification.Type)] = (int)notification.Type,
                    [nameof(DocumentChangeNotification.Key)] = notification.Key,
                    [nameof(DocumentChangeNotification.CollectionName)] = notification.CollectionName,
                    [nameof(DocumentChangeNotification.TypeName)] = notification.TypeName,
                    [nameof(DocumentChangeNotification.Etag)] = notification.Etag,
                },
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(value);
        }

        public void WatchOperation(long operationId)
        {
            _matchingOperations.TryAdd(operationId);
        }

        public void UnwatchOperation(long operationId)
        {
            _matchingOperations.TryRemove(operationId);
        }

        public void WatchAllOperations()
        {
            Interlocked.Increment(ref _watchAllOperations);
        }

        public void UnwatchAllOperations()
        {
            Interlocked.Decrement(ref _watchAllOperations);
        }

        public void SendOperationStatusChangeNotification(OperationStatusChangeNotification notification)
        {
            if (_watchAllOperations > 0)
            {
                Send(notification);
                return;
            }

            if (_matchingOperations.Contains(notification.OperationId))
            {
                Send(notification);
            }
        }

        private void Send(OperationStatusChangeNotification notification)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = "OperationStatusChangeNotification",
                ["Value"] = new DynamicJsonValue
                {
                    [nameof(OperationStatusChangeNotification.OperationId)] = (int)notification.OperationId,
                    [nameof(OperationStatusChangeNotification.State)] = notification.State.ToJson()
                },
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(value);
        }

        private void SendStartTime()
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = "ServerStartTimeNotification",
                ["Value"] = _documentDatabase.StartTime
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(value);
        }

        public void WatchAllAlerts()
        {
            Interlocked.Increment(ref _watchAllAlerts);
        }

        public void UnwatchAllAlerts()
        {
            Interlocked.Decrement(ref _watchAllAlerts);
        }

        public void SendAlertNotification(AlertNotification notification)
        {
            if (_watchAllAlerts > 0)
            {
                Send(notification);
            }
        }

        private void Send(AlertNotification notification)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = "AlertNotification",
                ["Value"] = new DynamicJsonValue
                {
                    [nameof(AlertNotification.Global)] = notification.Global,
                    [nameof(AlertNotification.Alert)] = notification.Alert.ToJson()
                }
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(value);
        }


        public async Task StartSendingNotifications(bool sendStartTime, bool throttleNotifications)
        {
            if (sendStartTime)
                SendStartTime();

            JsonOperationContext context;
            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                using (var ms = new MemoryStream())
                {
                    var sp = Stopwatch.StartNew();
                    while (true)
                    {
                        if (_disposeToken.IsCancellationRequested)
                            break;

                        sp.Restart();
                        ms.SetLength(0);
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            do
                            {
                                var value = await GetNextMessage(throttleNotifications);
                                if (_disposeToken.IsCancellationRequested)
                                    break;

                                context.Write(writer, value);
                                writer.WriteNewLine();

                                if (ms.Length > 16*1024)
                                    break;
                            } while (_sendQueue.Count > 0 || sp.Elapsed > _maxTimeToThorttleMessage);
                        }
                        if (ms.Length == 0)
                        {
                            // ensure that we send _something_ over the network, to keep the 
                            // connection alive
                            ms.WriteByte((byte)'\r');
                            ms.WriteByte((byte)'\n');
                        }

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _disposeToken.Token);
                    }
                }
            }
        }

        private readonly TimeSpan _maxTimeToThorttleMessage = TimeSpan.FromSeconds(5);
        private DateTime _lastMessage;
        private Task<DynamicJsonValue> _dequeueAsync;

        private async Task<DynamicJsonValue> GetNextMessage(bool throttleNotifications)
        {
            _dequeueAsync = _sendQueue.DequeueAsync();

            var dynamicJsonValue = await _dequeueAsync;

            if (throttleNotifications == false)
                return dynamicJsonValue;

            while (true)
            {
                var now = DateTime.UtcNow;
                var timeSinceLastMessage = now - _lastMessage;
                if (timeSinceLastMessage > _maxTimeToThorttleMessage)
                {
                    _lastMessage = now;
                    return dynamicJsonValue;
                }

                // we got more messages than we care to send, so we need to wait a maximum
                _dequeueAsync = _sendQueue.DequeueAsync();
                var waitResult = await Task.WhenAny(_dequeueAsync, Task.Delay(_maxTimeToThorttleMessage - timeSinceLastMessage));
                if (waitResult != _dequeueAsync)
                    break; // we hit the timeout, we can release the last recieved message

                dynamicJsonValue = await _dequeueAsync;
            }

            return dynamicJsonValue;
        }

        public void Dispose()
        {
            _disposeToken.Cancel();
            _sendQueue.Dispose();
        }

        public void Confirm(int commandId)
        {
            _sendQueue.Enqueue(new DynamicJsonValue
            {
                ["CommandId"] = commandId,
                ["Type"] = "Confirm"
            });
        }

        public void HandleCommand(string command, string commandParameter)
        {
            long commandParameterAsLong;
            long.TryParse(commandParameter, out commandParameterAsLong);

            /* if (Match(command, "watch-index"))
             {
                 WatchIndex(commandParameter);
             }
             else if (Match(command, "unwatch-index"))
             {
                 UnwatchIndex(commandParameter);
             }
             else if (Match(command, "watch-indexes"))
             {
                 WatchAllIndexes();
             }
             else if (Match(command, "unwatch-indexes"))
             {
                 UnwatchAllIndexes();
             }
             else if (Match(command, "watch-transformers"))
             {
                 WatchTransformers();
             }
             else if (Match(command, "unwatch-transformers"))
             {
                 UnwatchTransformers();
             }
             else*/
            if (Match(command, "watch-doc"))
            {
                WatchDocument(commandParameter);
            }
            else if (Match(command, "unwatch-doc"))
            {
                UnwatchDocument(commandParameter);
            }
            else if (Match(command, "watch-docs"))
            {
                WatchAllDocuments();
            }
            else if (Match(command, "unwatch-docs"))
            {
                UnwatchAllDocuments();
            }
            else if (Match(command, "watch-prefix"))
            {
                WatchDocumentPrefix(commandParameter);
            }
            else if (Equals(command, "unwatch-prefix"))
            {
                UnwatchDocumentPrefix(commandParameter);
            }
            else if (Match(command, "watch-collection"))
            {
                WatchDocumentInCollection(commandParameter);
            }
            else if (Equals(command, "unwatch-collection"))
            {
                UnwatchDocumentInCollection(commandParameter);
            }
            else if (Match(command, "watch-type"))
            {
                WatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "unwatch-type"))
            {
                UnwatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "watch-operation"))
            {
                WatchOperation(commandParameterAsLong);
            }
            else if (Equals(command, "unwatch-operation"))
            {
                UnwatchOperation(commandParameterAsLong);
            }
            else if (Equals(command, "watch-operations"))
            {
                WatchAllOperations();
            }
            else if (Equals(command, "unwatch-operations"))
            {
                UnwatchAllOperations();
            }
            else if (Equals(command, "watch-alerts"))
            {
                WatchAllAlerts();
            }
            else if (Equals(command, "unwatch-alerts"))
            {
                UnwatchAllAlerts();
            }
            /*else if (Match(command, "watch-replication-conflicts"))
            {
                WatchAllReplicationConflicts();
            }
            else if (Match(command, "unwatch-replication-conflicts"))
            {
                UnwatchAllReplicationConflicts();
            }
            else if (Match(command, "watch-bulk-operation"))
            {
                WatchBulkInsert(commandParameter);
            }
            else if (Match(command, "unwatch-bulk-operation"))
            {
                UnwatchBulkInsert(commandParameter);
            }
            else if (Match(command, "watch-data-subscriptions"))
            {
                WatchAllDataSubscriptions();
            }
            else if (Match(command, "unwatch-data-subscriptions"))
            {
                UnwatchAllDataSubscriptions();
            }
            else if (Match(command, "watch-data-subscription"))
            {
                WatchDataSubscription(long.Parse(commandParameter));
            }
            else if (Match(command, "unwatch-data-subscription"))
            {
                UnwatchDataSubscription(long.Parse(commandParameter));
            }*/
            else
            {
                throw new ArgumentOutOfRangeException(nameof(command), "Command argument is not valid");
            }
        }

        protected static bool Match(string x, string y)
        {
            return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
        }

        public DynamicJsonValue GetDebugInfo()
        {
            return new DynamicJsonValue
            {
                ["Id"] = Id,
                ["State"] = _webSocket.State.ToString(),
                ["CloseStatus"] = _webSocket.CloseStatus,
                ["CloseStatusDescription"] = _webSocket.CloseStatusDescription,
                ["SubProtocol"] = _webSocket.SubProtocol,
                ["Age"] = Age,
                ["WatchAllDocuments"] = _watchAllDocuments > 0,
                ["WatchAllIndexes"] = false,
                ["WatchAllTransformers"] = false,
                /*["WatchConfig"] = _watchConfig > 0,
                ["WatchConflicts"] = _watchConflicts > 0,
                ["WatchSync"] = _watchSync > 0,*/
                ["WatchDocumentPrefixes"] = _matchingDocumentPrefixes.ToArray(),
                ["WatchDocumentsInCollection"] = _matchingDocumentsInCollection.ToArray(),
                ["WatchIndexes"] = _matchingIndexes.ToArray(),
                ["WatchDocuments"] = _matchingDocuments.ToArray(),
            };
        }
    }
}