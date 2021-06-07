using System;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Json;

namespace Sparrow.Server.Json.Sync
{
    public unsafe class BlittableJsonTextWriter : AbstractBlittableJsonTextWriter, IDisposable
    {
        public BlittableJsonTextWriter(JsonOperationContext context, Stream stream) : base(context, stream)
        {
        }

        public void Dispose()
        {
            DisposeInternal();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush()
        {
            FlushInternal();
        }
    }
}
