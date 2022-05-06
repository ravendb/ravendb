using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Mime;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Core;
using Sparrow.Json;
using Sparrow.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.Queue
{
    public class BlittableEventBinaryFormatter : CloudEventFormatter, IDisposable
    {
        private readonly JsonOperationContext _ctx;
        private readonly List<MemoryStream> _streams = new();

        public BlittableEventBinaryFormatter(JsonOperationContext ctx)
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

            MemoryStream ms = _ctx.CheckoutMemoryStream();

            _streams.Add(ms)
;

            using (var writer = new BlittableJsonTextWriter(_ctx, ms))
            {
                var data = (BlittableJsonReaderObject)cloudEvent.Data;

                _ctx.Write(writer, data);

                writer.Flush();

                ms.TryGetBuffer(out var buffer);

                Memory<byte> encodeBinaryModeEventData = buffer.AsMemory();

                return encodeBinaryModeEventData;
            }
        }

        public override CloudEvent DecodeStructuredModeMessage(ReadOnlyMemory<byte> body, ContentType? contentType, IEnumerable<CloudEventAttribute>? extensionAttributes)
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


        public override IReadOnlyList<CloudEvent> DecodeBatchModeMessage(ReadOnlyMemory<byte> body, ContentType? contentType, IEnumerable<CloudEventAttribute>? extensionAttributes)
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
                _ctx.ReturnMemoryStream(ms);
            }
        }
    }
}
