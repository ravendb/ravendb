using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Sparrow.Server.Json.Sync
{
    internal class RachisBlittableJsonTextWriter : BlittableJsonTextWriter
    {
        private readonly Action _afterFlush;

        public RachisBlittableJsonTextWriter(JsonOperationContext context, Stream stream, Action afterFlush) : base(context, stream)
        {
            _afterFlush = afterFlush;
        }

        public override void Flush()
        {
            base.Flush();
            _afterFlush?.Invoke();
        }
    }

    public class BlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IDisposable
    {
        public BlittableJsonTextWriter(JsonOperationContext context, Stream stream) : base(context, stream)
        {
        }

        public void Dispose()
        {
            DisposeInternal();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void Flush()
        {
            FlushInternal();
        }
    }
}
