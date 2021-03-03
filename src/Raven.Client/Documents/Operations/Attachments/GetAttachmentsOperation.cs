using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class GetAttachmentsOperation : IOperation<IEnumerator<AttachmentEnumeratorResult>>
    {
        private readonly AttachmentType _type;
        private readonly IEnumerable<AttachmentRequest> _attachments;

        public GetAttachmentsOperation(IEnumerable<AttachmentRequest> attachments, AttachmentType type)
        {
            _type = type;
            _attachments = attachments;
        }

        public RavenCommand<IEnumerator<AttachmentEnumeratorResult>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentsCommand(context, _attachments, _type);
        }

        internal class GetAttachmentsCommand : RavenCommand<IEnumerator<AttachmentEnumeratorResult>>
        {
            private readonly JsonOperationContext _context;
            private readonly AttachmentType _type;
            internal IEnumerable<AttachmentRequest> Attachments { get; }
            internal List<AttachmentDetails> AttachmentsMetadata { get; } = new List<AttachmentDetails>();

            public GetAttachmentsCommand(JsonOperationContext context, IEnumerable<AttachmentRequest> attachments, AttachmentType type)
            {
                _context = context;
                _type = type;
                Attachments = attachments;
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments/bulk";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        await using (var writer = new AsyncBlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName(nameof(AttachmentType));
                            writer.WriteString(_type.ToString());
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(Attachments));

                            writer.WriteStartArray();
                            var first = true;
                            foreach (var attachment in Attachments)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteStartObject();
                                writer.WritePropertyName(nameof(AttachmentRequest.DocumentId));
                                writer.WriteString(attachment.DocumentId);
                                writer.WriteComma();
                                writer.WritePropertyName(nameof(AttachmentRequest.Name));
                                writer.WriteString(attachment.Name);
                                writer.WriteEndObject();
                            }
                            writer.WriteEndArray();

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                var state = new JsonParserState();
                Stream stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                using (context.GetMemoryBuffer(out var buffer))
                using (var parser = new UnmanagedJsonParser(context, state, "attachments/receive"))
                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "attachments/list", parser, state))
                using (var peepingTomStream = new PeepingTomStream(stream, context))
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                        throw new Exception("cannot parse stream");

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                        throw new Exception($"Expected token {nameof(JsonParserToken.StartObject)}, but got {nameof(state.CurrentTokenType)}.");

                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);
                    using (var data = builder.CreateReader())
                    {
                        if (data.TryGetMember(nameof(AttachmentsMetadata), out object obj) && obj is BlittableJsonReaderArray bjra)
                        {
                            foreach (BlittableJsonReaderObject e in bjra)
                            {
                                var cur = JsonDeserializationClient.AttachmentDetails(e);
                                AttachmentsMetadata.Add(cur);
                            }
                        }
                    }

                    var bufferSize = parser.BufferSize - parser.BufferOffset;
                    var copy = ArrayPool<byte>.Shared.Rent(bufferSize);
                    var copyMemory = new Memory<byte>(copy);
                    buffer.Memory.Memory.Slice(parser.BufferOffset, bufferSize).CopyTo(copyMemory);

                    Result = Iterate(stream, copy, bufferSize).GetEnumerator();
                }

                return ResponseDisposeHandling.Manually;
            }

            private IEnumerable<AttachmentEnumeratorResult> Iterate(Stream stream, byte[] copy, int bufferSize)
            {
                LimitedStream prev = null;

                using (var cs = new ConcatStream(new ConcatStream.RentedBuffer
                {
                    Buffer = copy,
                    Count = bufferSize,
                    Offset = 0
                }, stream))
                {
                    long position = 0;

                    foreach (var attachment in AttachmentsMetadata)
                    {
                        prev?.Dispose();

                        prev = new LimitedStream(cs, attachment.Size, position, prev?.OverallRead ?? 0);
                        position += attachment.Size;

                        yield return new AttachmentEnumeratorResult(attachment, prev);
                    }

                    // mark the last attachment as disposed
                    prev?.Dispose();
                }
            }

            public override bool IsReadRequest => true;
        }
    }
}
