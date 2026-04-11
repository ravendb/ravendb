using System;
using Microsoft.IO;
using Sparrow;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed class SystemTextJsonBlittableReader : IJsonReader
    {
        private RecyclableMemoryStream _stream;

        public void Initialize(BlittableJsonReaderObject blittable)
        {
            _stream?.Dispose();
            _stream = RecyclableMemoryStreamFactory.GetRecyclableStream();

            blittable.WriteJsonTo(_stream);
        }

        public ReadOnlySpan<byte> GetUtf8Json()
        {
            _stream.TryGetBuffer(out ArraySegment<byte> buffer);
            return new ReadOnlySpan<byte>(buffer.Array, buffer.Offset, buffer.Count);
        }

        public void Dispose()
        {
            _stream?.Dispose();
            _stream = null;
        }
    }
}
