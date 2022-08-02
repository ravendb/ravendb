using System;
using System.IO;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Utils
{
    public class AsyncBlittableJsonTextWriterForDebug : AsyncBlittableJsonTextWriter
    {
        private readonly ServerStore _serverStore;
        private bool _isFirst = true;
        private bool _isOnlyWrite;

        public AsyncBlittableJsonTextWriterForDebug(JsonOperationContext context, ServerStore serverStore, Stream stream) : base(context, stream)
        {
            _serverStore = serverStore;
        }
        
        public override void WriteStartObject()
        {
            base.WriteStartObject();

            if (_isFirst)
            {
                _isFirst = false;

                WritePropertyName("@Metadata");
                WriteStartObject();
                WritePropertyName(nameof(DateTime));
                WriteDateTime(DateTime.UtcNow, true);
                WriteComma();
                WritePropertyName(nameof(_serverStore.Server.WebUrl));
                WriteString(_serverStore.Server.WebUrl);
                WriteComma();
                WritePropertyName(nameof(_serverStore.NodeTag));
                WriteString(_serverStore.NodeTag);

                WriteEndObject();

                _isOnlyWrite = true;
            }
        }

        protected override void EnsureBuffer(int len)
        {
            base.EnsureBuffer(len);

            if (_isOnlyWrite)
            {
                _isOnlyWrite = false;
                WriteComma();
            }
        }

        public override void WriteEndObject()
        {
            _isOnlyWrite = false;
            base.WriteEndObject();
        }
    }
}
