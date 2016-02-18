using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class NotificationsClientConnection : IDisposable
    {
        private readonly WebSocket _webSocket;
        private readonly DocumentDatabase _documentDatabase;
        private readonly BlockingCollection<DynamicJsonValue> _sendQueue = new BlockingCollection<DynamicJsonValue>();
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();

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

        private readonly ConcurrentSet<string> _matchingBulkInserts =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private int watchAllDocuments;
        private int watchAllIndexes;
        private int watchAllTransformers;
        private int watchAllReplicationConflicts;
        private int watchAllDataSubscriptions;

        public NotificationsClientConnection(WebSocket webSocket, DocumentDatabase documentDatabase)
        {
            _webSocket = webSocket;
            _documentDatabase = documentDatabase;
        }

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
            Interlocked.Increment(ref watchAllDocuments);
        }

        public void UnwatchAllDocuments()
        {
            Interlocked.Decrement(ref watchAllDocuments);
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
            if (watchAllDocuments > 0)
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
                    ["Type"] = (int)notification.Type,
                    ["Key"] = notification.Key,
                    ["CollectionName"] = notification.CollectionName,
                    ["TypeName"] = notification.TypeName,
                    ["Etag"] = notification.Etag,
                },
            };

            _sendQueue.Add(value, _disposeToken.Token);
        }

        public async Task StartSendingNotifications()
        {
            MemoryOperationContext context;
            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var buffer = context.GetManagedBuffer();
                using (var ms = new MemoryStream(buffer))
                {
                    while (true)
                    {
                        if (_disposeToken.IsCancellationRequested)
                            break;

                        ms.Position = 0;
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            DynamicJsonValue value;
                            try
                            {
                                value = _sendQueue.Take(_disposeToken.Token);
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }
                            context.Write(writer, value);
                            writer.Flush();

                            while (_sendQueue.TryTake(out value) &&
                                   ms.Position < 4096 - 256)
                            {
                                context.Write(writer, value);
                                writer.Flush();
                            }
                        }

                        await _webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, (int) ms.Position), WebSocketMessageType.Text, true, _disposeToken.Token);
                    }
                }
            }
        }

        public void Dispose()
        {
            _disposeToken.Cancel();
        }
    }
}