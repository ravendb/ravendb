using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments
{
    public class GetAttachmentsOperation : IOperation<IEnumerator<AttachmentEnumeratorResult>>
    {
        private readonly AttachmentType _type;
        private readonly IEnumerable<KeyValuePair<string, string>> _attachments;

        public GetAttachmentsOperation(IEnumerable<KeyValuePair<string, string>> attachments, AttachmentType type)
        {
            _type = type;
            _attachments = attachments;
        }

        public RavenCommand<IEnumerator<AttachmentEnumeratorResult>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentsCommand(context, _attachments, _type);
        }

        private class GetAttachmentsCommand : RavenCommand<IEnumerator<AttachmentEnumeratorResult>>
        {
            private readonly JsonOperationContext _context;
            private readonly AttachmentType _type;
            private readonly IEnumerable<KeyValuePair<string, string>> _attachments;

            public GetAttachmentsCommand(JsonOperationContext context, IEnumerable<KeyValuePair<string, string>> attachments, AttachmentType type)
            {
                _context = context;
                _type = type;
                _attachments = attachments;
                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments/list";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        using (var writer = new BlittableJsonTextWriter(_context, stream))
                        {
                            writer.WriteStartObject();

                            writer.WritePropertyName("Type");
                            writer.WriteString(_type.ToString());
                            writer.WriteComma();

                            writer.WritePropertyName("Attachments");

                            writer.WriteStartArray();
                            var first = true;
                            foreach (var kvp in _attachments)
                            {
                                if (first == false)
                                    writer.WriteComma();
                                first = false;

                                writer.WriteStartObject();
                                writer.WritePropertyName("Key");
                                writer.WriteString(kvp.Key);
                                writer.WriteComma();
                                writer.WritePropertyName("Value");
                                writer.WriteString(kvp.Value);
                                writer.WriteEndObject();;

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
                AttachmentsDetails attachmentsMetadata;
                AttachmentsStreamInfo streamInfo;
                var state = new JsonParserState();
                Stream stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                using (context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
                using (var parser = new UnmanagedJsonParser(context, state, "attachments/receive"))
                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "attachments/list", parser, state))
                using (var peepingTomStream = new PeepingTomStream(stream, context))
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                        throw new Exception("cannot parse stream");

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                        throw new Exception($"Expected token {nameof(JsonParserToken.StartObject)}, but got {nameof(state.CurrentTokenType)}.");

                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);

                    BlittableJsonReaderObject data = builder.CreateReader();
                    attachmentsMetadata = JsonDeserializationClient.AttachmentAdvancedDetails(data);

                    var bufferSize = parser.BufferSize - parser.BufferOffset;
                    var tmpBuffer = new byte[bufferSize];
                    Array.Copy(buffer.Buffer.Array ?? throw new InvalidOperationException(), buffer.Buffer.Offset + parser.BufferOffset, tmpBuffer, 0, bufferSize);

                    streamInfo = new AttachmentsStreamInfo
                    {
                        AttachmentAdvancedDetails = attachmentsMetadata,
                        Buffer = tmpBuffer
                    };
                }

                Result = attachmentsMetadata.AttachmentsMetadata.Select(attachment => new AttachmentEnumeratorResult(new AttachmentsStream(stream, attachment.Name, attachment.Index, attachment.Size, streamInfo))
                {
                    Details = attachment
                }).GetEnumerator();


                return ResponseDisposeHandling.Manually;
            }

            public override bool IsReadRequest => true;
        }
    }
}
