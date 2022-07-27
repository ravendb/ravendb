using System;
using System.IO;
using Sparrow.Json;
using Sparrow.Json.Sync;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Rachis.Json.Sync
{
    internal class RachisBlittableJsonTextWriter : BlittableJsonTextWriter
    {
        private readonly Action _afterFlush;

        public RachisBlittableJsonTextWriter(JsonOperationContext context, Stream stream, Action afterFlush) : base(context, stream)
        {
            _afterFlush = afterFlush;
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
