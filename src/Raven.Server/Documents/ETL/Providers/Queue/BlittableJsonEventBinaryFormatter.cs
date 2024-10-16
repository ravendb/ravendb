using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Mime;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Core;
using Microsoft.IO;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.Queue
{
    public sealed class BlittableJsonEventBinaryFormatter : CloudEventFormatter, IDisposable
    {
        private readonly JsonOperationContext _ctx;
        private readonly List<MemoryStream> _streams = new();

        public BlittableJsonEventBinaryFormatter(JsonOperationContext ctx)
        {
            _ctx = ctx;
        }

        public override ReadOnlyMemory<byte> EncodeBinaryModeEventData(CloudEvent cloudEvent)
        {
            Validation.CheckCloudEventArgument(cloudEvent, nameof(cloudEvent));

            if (cloudEvent.Data is null)
            {
                return Array.Empty<byte>();
            }

            var ms = RecyclableMemoryStreamFactory.GetRecyclableStream();

            _streams.Add(ms);

            using (var writer = new BlittableJsonTextWriter(_ctx, ms))
            {
                var data = (BlittableJsonReaderObject)cloudEvent.Data;

                _ctx.Write(writer, data);
            }

            ms.TryGetBuffer(out var buffer);

            return buffer;
        }

        public override CloudEvent DecodeStructuredModeMessage(ReadOnlyMemory<byte> body, ContentType contentType, IEnumerable<CloudEventAttribute> extensionAttributes)
        {
            throw new NotImplementedException();
        }

        public override ReadOnlyMemory<byte> EncodeStructuredModeMessage(CloudEvent cloudEvent, out ContentType contentType)
        {
            throw new NotImplementedException();
        }

        public override void DecodeBinaryModeEventData(ReadOnlyMemory<byte> body, CloudEvent cloudEvent)
        {
            throw new NotImplementedException();
        }


        public override IReadOnlyList<CloudEvent> DecodeBatchModeMessage(ReadOnlyMemory<byte> body, ContentType contentType, IEnumerable<CloudEventAttribute> extensionAttributes)
        {
            throw new NotImplementedException();
        }

        public override ReadOnlyMemory<byte> EncodeBatchModeMessage(IEnumerable<CloudEvent> cloudEvents, out ContentType contentType)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            foreach (MemoryStream ms in _streams)
            {
                ms.Dispose();
            }
        }
    }
}
