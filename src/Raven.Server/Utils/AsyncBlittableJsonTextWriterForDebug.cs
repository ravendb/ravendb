using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    public sealed class AsyncBlittableJsonTextWriterForDebug : IBlittableJsonTextWriter, IAsyncDisposable
    {
        private readonly ServerStore _serverStore;
        private bool _isFirst = true;
        private bool _isOnlyWrite;
        private readonly AsyncBlittableJsonTextWriter _inner;

        public AsyncBlittableJsonTextWriterForDebug(JsonOperationContext context, ServerStore serverStore, Stream stream)
        {
            _isFirst = true;
            _serverStore = serverStore;
            _inner = new AsyncBlittableJsonTextWriter(context, stream);
        }

        public void WriteStartObject()
        {
            _inner.WriteStartObject();

            if (_isFirst)
            {
                _isFirst = false;

                _inner.WritePropertyName(Constants.Documents.Metadata.Key);
                _inner.WriteStartObject();
                _inner.WritePropertyName(nameof(DateTime));
                _inner.WriteDateTime(DateTime.UtcNow, true);
                _inner.WriteComma();
                _inner.WritePropertyName(nameof(_serverStore.Server.WebUrl));
                _inner.WriteString(_serverStore.Server.WebUrl);
                _inner.WriteComma();
                _inner.WritePropertyName(nameof(_serverStore.NodeTag));
                _inner.WriteString(_serverStore.NodeTag);
                _inner.WriteEndObject();

                _isOnlyWrite = true;
            }
        }

        public void WriteEndObject()
        {
            _isOnlyWrite = false;
            _inner.WriteEndObject();
        }

        public void WriteObject(BlittableJsonReaderObject obj)
        {
            if (_isOnlyWrite)
            {
                _isOnlyWrite = false;
                WriteComma();
            }
            _inner.WriteObject(obj);
        }

        public void WriteValue(BlittableJsonToken token, object val)
        {
            _inner.WriteValue(token, val);
        }

        public int WriteDateTime(DateTime? value, bool isUtc)
        {
            return _inner.WriteDateTime(value, isUtc);
        }

        public int WriteDateTime(DateTime value, bool isUtc)
        {
            return _inner.WriteDateTime(value, isUtc);
        }

        public void WriteString(string str, bool skipEscaping = false)
        {
            _inner.WriteString(str, skipEscaping);
        }

        public void WriteString(LazyStringValue str, bool skipEscaping = false)
        {
            _inner.WriteString(str, skipEscaping);
        }

        public void WriteString(LazyCompressedStringValue str)
        {
            _inner.WriteString(str);
        }

        public void WriteStartArray()
        {
            _inner.WriteStartArray(); 
        }

        public void WriteEndArray()
        {
            _inner.WriteEndArray();
        }

        public void WriteNull()
        {
            _inner.WriteNull();
        }

        public void WriteBool(bool val)
        {
            _inner.WriteBool(val);
        }

        public void WriteComma()
        {
            _inner.WriteComma();
        }

        public void WriteInteger(long val)
        {
            _inner.WriteInteger(val);
        }

        public void WriteDouble(LazyNumberValue val)
        {
            _inner.WriteDouble(val);
        }

        public void WriteDouble(double val)
        {
            _inner.WriteDouble(val);
        }

        public void WriteNewLine()
        {
            _inner.WriteNewLine();
        }

        public void WritePropertyName(ReadOnlySpan<byte> prop)
        {
            if (_isOnlyWrite)
            {
                _isOnlyWrite = false;
                WriteComma();
            }
            _inner.WritePropertyName(prop);
        }

        public void WritePropertyName(string prop)
        {
            if (_isOnlyWrite)
            {
                _isOnlyWrite = false;
                WriteComma();
            }
            _inner.WritePropertyName(prop);
        }

        public void WritePropertyName(StringSegment prop)
        {
            if (_isOnlyWrite)
            {
                _isOnlyWrite = false;
                WriteComma();
            }
            _inner.WritePropertyName(prop);
        }

        public ValueTask<int> MaybeFlushAsync(CancellationToken token = default)
        {
            return _inner.MaybeFlushAsync(token);
        }

        public ValueTask DisposeAsync()
        {
            return _inner.DisposeAsync();
        }
    }
}
