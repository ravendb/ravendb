﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Changes;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Client.Documents.Changes
{
    public class DatabaseChanges : IDatabaseChanges, IDisposable
    {
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        private readonly MemoryStream _ms = new MemoryStream();

        private readonly RequestExecutor _requestExecutor;
        private readonly DocumentConventions _conventions;
        private readonly Uri _url;

        private readonly Action _onDispose;
        private readonly ClientWebSocket _client;

        private readonly Task _task;
        private readonly CancellationTokenSource _cts;

        private readonly AtomicDictionary<DatabaseConnectionState> _counters = new AtomicDictionary<DatabaseConnectionState>(StringComparer.OrdinalIgnoreCase);

        public DatabaseChanges(RequestExecutor requestExecutor, DocumentConventions conventions, string databaseName, Action onDispose)
        {
            _requestExecutor = requestExecutor;
            _conventions = conventions;

            var url = $"{requestExecutor.Url}/databases/{databaseName}/changes"
                .ToLower()
                .Replace("http://", "ws://")
                .Replace("https://", "wss://");

            _url = new Uri(url, UriKind.Absolute);

            _cts = new CancellationTokenSource();
            _client = new ClientWebSocket();

            _onDispose = onDispose;

            _task = DoWork();
        }

        public bool Connected { get; private set; }
        public event EventHandler ConnectionStatusChanged;

        public IObservable<IndexChange> ForIndex(string indexName)
        {
            var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", indexName);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Name, indexName, StringComparison.OrdinalIgnoreCase));

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForDocument(string docId)
        {
            var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Key, docId, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForAllDocuments()
        {
            var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<OperationStatusChange> ForOperationId(long operationId)
        {
            var counter = GetOrAddConnectionState("operations/" + operationId, "watch-operation", "unwatch-operation", operationId.ToString());

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<OperationStatusChange> ForAllOperations()
        {
            var counter = GetOrAddConnectionState("all-operations", "watch-operations", "unwatch-operations", null);

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnOperationStatusChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<IndexChange> ForAllIndexes()
        {
            var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", null);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnIndexChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<TransformerChange> ForAllTransformers()
        {
            var counter = GetOrAddConnectionState("all-transformers", "watch-transformers", "unwatch-transformers", null);

            var taskedObservable = new ChangesObservable<TransformerChange, DatabaseConnectionState>(
                counter,
                notification => true);

            counter.OnTransformerChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForDocumentsStartingWith(string docIdPrefix)
        {
            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", docIdPrefix);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => notification.Key != null && notification.Key.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            if (collectionName == null)
                throw new ArgumentNullException(nameof(collectionName));

            var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", collectionName);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForDocumentsInCollection<TEntity>()
        {
            var collectionName = _conventions.GetCollectionName(typeof(TEntity));
            return ForDocumentsInCollection(collectionName);
        }

        public IObservable<DocumentChange> ForDocumentsOfType(string typeName)
        {
            if (typeName == null) throw new ArgumentNullException(nameof(typeName));
            var encodedTypeName = Uri.EscapeDataString(typeName);

            var counter = GetOrAddConnectionState("types/" + typeName, "watch-type", "unwatch-type", encodedTypeName);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(typeName, notification.TypeName, StringComparison.OrdinalIgnoreCase));

            counter.OnDocumentChangeNotification += taskedObservable.Send;
            counter.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservable<DocumentChange> ForDocumentsOfType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var typeName = _conventions.FindClrTypeName(type);
            return ForDocumentsOfType(typeName);
        }

        public IObservable<DocumentChange> ForDocumentsOfType<TEntity>()
        {
            var typeName = _conventions.FindClrTypeName(typeof(TEntity));
            return ForDocumentsOfType(typeName);
        }

        public event Action<Exception> OnError;

        public void Dispose()
        {
            _cts.Cancel();

            _task.Wait();
            _client?.Dispose();

            ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

            _onDispose?.Invoke();
        }

        private DatabaseConnectionState GetOrAddConnectionState(string name, string watchCommand, string unwatchCommand, string value)
        {
            var counter = _counters.GetOrAdd(name, s =>
            {
                async Task OnDisconnect()
                {
                    await Send(unwatchCommand, value).ConfigureAwait(false);
                    _counters.Remove(s);
                }

                Task OnConnect()
                {
                    return Send(watchCommand, value);
                }

                return new DatabaseConnectionState(OnConnect, OnDisconnect);
            });

            return counter;
        }

        private async Task Send(string command, string value)
        {
            await _semaphore.WaitAsync(_cts.Token).ConfigureAwait(false);

            try
            {
                JsonOperationContext context;
                using (_requestExecutor.ContextPool.AllocateOperationContext(out context))
                using (var writer = new BlittableJsonTextWriter(context, _ms))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Command");
                    writer.WriteString(command);
                    writer.WriteComma();

                    writer.WritePropertyName("Param");
                    writer.WriteString(value);

                    writer.WriteEndObject();
                }

                ArraySegment<byte> buffer;
                _ms.TryGetBuffer(out buffer);

                await _client.SendAsync(buffer, WebSocketMessageType.Text, endOfMessage: true, cancellationToken: _cts.Token);
            }
            finally
            {
                _semaphore.Release();
                _ms.SetLength(0);
            }
        }

        private async Task DoWork()
        {
            while (_cts.IsCancellationRequested == false)
            {
                try
                {
                    if (Connected == false)
                    {
                        await _client.ConnectAsync(_url, _cts.Token).ConfigureAwait(false);
                        Connected = true;
                        ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

                        await Subscribe().ConfigureAwait(false);
                    }

                    await ProcessChanges().ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    Connected = false;
                    ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);
                    return;
                }
                catch (ChangeProcessingException e)
                {
                    NotifyAboutError(e);
                    continue;
                }
                catch (Exception e)
                {
                    Connected = false;
                    ConnectionStatusChanged?.Invoke(this, EventArgs.Empty);

                    NotifyAboutError(e);
                }

                try
                {
                    await TimeoutManager.WaitFor(TimeSpan.FromSeconds(15), _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        private async Task ProcessChanges()
        {
            JsonOperationContext context;
            using (_requestExecutor.ContextPool.AllocateOperationContext(out context))
            {
                while (_cts.IsCancellationRequested == false)
                {
                    var state = new JsonParserState();

                    JsonOperationContext.ManagedPinnedBuffer buffer;
                    using (var stream = new WebSocketStream(_client, _cts.Token))
                    using (context.GetManagedBuffer(out buffer))
                    using (var parser = new UnmanagedJsonParser(context, state, "changes/receive"))
                    using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "readArray/singleResult", parser, state))
                    {
                        if (await UnmanagedJsonParserHelper.ReadAsync(stream, parser, state, buffer).ConfigureAwait(false) == false)
                            continue;

                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            continue;

                        while (true)
                        {
                            if (await UnmanagedJsonParserHelper.ReadAsync(stream, parser, state, buffer).ConfigureAwait(false) == false)
                                continue;

                            if (state.CurrentTokenType == JsonParserToken.EndArray)
                                break;

                            builder.Renew("changes/receive", BlittableJsonDocumentBuilder.UsageMode.None);

                            await UnmanagedJsonParserHelper.ReadObjectAsync(builder, stream, parser, buffer).ConfigureAwait(false);

                            var json = builder.CreateReader();

                            try
                            {
                                if (json.TryGet("Type", out string type) == false)
                                    continue;

                                switch (type)
                                {
                                    case "Error":
                                        json.TryGet("Exception", out string exceptionAsString);
                                        NotifyAboutError(new Exception(exceptionAsString));
                                        break;
                                    default:
                                        BlittableJsonReaderObject value;
                                        json.TryGet("Value", out value);
                                        NotifySubscribers(type, value, _counters.ValuesSnapshot);
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                throw new ChangeProcessingException(e);
                            }
                        }
                    }
                }
            }
        }

        private void NotifySubscribers(string type, BlittableJsonReaderObject value, List<DatabaseConnectionState> states)
        {
            switch (type)
            {
                case nameof(DocumentChange):
                    var documentChange = DocumentChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(documentChange);
                    }
                    break;
                case nameof(IndexChange):
                    var indexChange = IndexChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(indexChange);
                    }
                    break;
                case nameof(TransformerChange):
                    var transformerChange = TransformerChange.FromJson(value);
                    foreach (var state in states)
                    {
                        state.Send(transformerChange);
                    }
                    break;
                case nameof(OperationStatusChange):
                    var operationStatusChange = OperationStatusChange.FromJson(value, _conventions);
                    foreach (var state in states)
                    {
                        state.Send(operationStatusChange);
                    }
                    break;
                default:
                    throw new NotSupportedException(type);
            }
        }

        private async Task Subscribe()
        {
            foreach (var state in _counters.ValuesSnapshot)
            {
                await state.OnConnect().ConfigureAwait(false);
            }
        }

        private void NotifyAboutError(Exception e)
        {
            OnError?.Invoke(e);

            foreach (var state in _counters.ValuesSnapshot)
            {
                state.Error(e);
            }
        }
    }
}