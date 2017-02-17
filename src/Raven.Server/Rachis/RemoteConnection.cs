using System;
using System.IO;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Rachis
{
    public class RemoteConnection : IDisposable
    {
        private readonly TransactionContextPool _pool;
        private readonly Stream _stream;
        private readonly JsonOperationContext.ManagedPinnedBuffer _buffer;
        private string _debugSource;
        private Logger _log;

        public string DebugSource => _debugSource;

        public RemoteConnection(TransactionContextPool pool, Stream stream)
        {
            _pool = pool;
            _stream = stream;
            _buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance();
        }

        public void Send(Exception e)
        {
            if (_log?.IsInfoEnabled == true)
            {
                _log.Info("Sending an error (and aborting connection)", e);
            }

            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        ["Type"] = "Error",
                        ["Message"] = e.Message,
                        ["Exception"] = e.ToString()
                    });
            }
        }

        public RachisEntry ReadSingleEntry(JsonOperationContext context)
        {
            var json = context.ParseToMemory(_stream, "rachis-entry",
                BlittableJsonDocumentBuilder.UsageMode.None, _buffer);
            json.BlittableValidation();
            return JsonDeserializationRachis.RachisEntry(json);
        }

        public AppendEntries ReadAppendEntries()
        {
            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
                return ReadAppendEntries(context);
        }

        public AppendEntries ReadAppendEntries(JsonOperationContext context)
        {
            using (
                var json = context.ParseToMemory(_stream, "rachis-append-entries",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(AppendEntries), json);
                return JsonDeserializationRachis.AppendEntries(json);
            }
        }

        public InstallSnapshot ReadInstallSnapshot()
        {
            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            using (
                var json = context.ParseToMemory(_stream, "rachis-install-snapshot",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(AppendEntries), json);
                return JsonDeserializationRachis.InstallSnapshot(json);
            }
        }

        public void Send(AppendEntriesResponse aer)
        {
            if (_log?.IsInfoEnabled == true)
            {
                if (aer.Message != null)
                {
                    _log.Info($"Replying with success {aer.Success}: {aer.Message}");
                }
            }
            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, _stream))
            {
                var msg = new DynamicJsonValue
                {
                    ["Type"] = "AppendEntriesResponse",
                    [nameof(AppendEntriesResponse.Success)] = aer.Success,
                    [nameof(AppendEntriesResponse.Message)] = aer.Message,
                    [nameof(AppendEntriesResponse.CurrentTerm)] = aer.CurrentTerm,
                    [nameof(AppendEntriesResponse.LastLogIndex)] = aer.LastLogIndex,
                };
                var negotiation = aer.Negotiation;
                if (negotiation != null)
                {
                    msg[nameof(AppendEntriesResponse.Negotiation)] = new DynamicJsonValue
                    {
                        [nameof(Negotiation.MaxIndex)] = negotiation.MaxIndex,
                        [nameof(Negotiation.MinIndex)] = negotiation.MinIndex,
                        [nameof(Negotiation.MidpointIndex)] = negotiation.MidpointIndex,
                        [nameof(Negotiation.MidpointTerm)] = negotiation.MidpointTerm,
                    };
                }
                context.Write(writer, msg);
            }
        }

        public void Dispose()
        {
            _buffer?.Dispose();
        }

        public RachisHello Init()
        {
            JsonOperationContext context;
            using (_pool.AllocateOperationContext(out context))
            using (
                var json = context.ParseToMemory(_stream, "rachis-initial-msg",
                    BlittableJsonDocumentBuilder.UsageMode.None, _buffer))
            {
                json.BlittableValidation();
                ValidateMessage(nameof(RachisHello), json);
                var rachisHello = JsonDeserializationRachis.RachisHello(json);
                _debugSource = rachisHello.DebugSourceIdentifier ?? "unknown";
                _log = LoggingSource.Instance.GetLogger<RemoteConnection>(_debugSource);
                return rachisHello;
            }
        }

        public override string ToString()
        {
            return _debugSource;
        }

        private static void ValidateMessage(string expectedType,BlittableJsonReaderObject json)
        {
            string type;
            if (json.TryGet("Type", out type) == false || type != expectedType)
                ThrowUnexpectedMessage(expectedType, json);
        }

        private static void ThrowUnexpectedMessage(string expectedType, BlittableJsonReaderObject json)
        {
            throw new InvalidOperationException(
                $"Expected to get type of \'{expectedType}\' message, but got unkonwn message: {json}");
        }
    }
}