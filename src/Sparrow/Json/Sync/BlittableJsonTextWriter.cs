using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace Sparrow.Json.Sync
{
    internal sealed class BlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IDisposable
    {
        public BlittableJsonTextWriter(JsonOperationContext context, Stream stream) : base(context, stream)
        {
        }

        public void Dispose()
        {
            DisposeInternal();
        }

        public void Flush()
        {
            FlushInternal();
        }
    }
}
