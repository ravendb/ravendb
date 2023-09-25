using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Server.Rachis.Json.Sync
{
    internal sealed class RachisBlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IDisposable
    {
        private readonly Action _afterFlush;

        public RachisBlittableJsonTextWriter(JsonOperationContext context, Stream stream, Action afterFlush) : base(context, stream)
        {
            _afterFlush = afterFlush;
        }

        public void Dispose()
        {
            DisposeInternal();
        }

        public void Flush()
        {
            FlushInternal();
        }

        protected override bool FlushInternal()
        {
            var flushed = base.FlushInternal();
            if (flushed)
                _afterFlush?.Invoke();

            return flushed;
        }
    }
}
