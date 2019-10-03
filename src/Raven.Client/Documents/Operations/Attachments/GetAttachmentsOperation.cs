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
    public class GetAttachmentsOperation : IOperation<Dictionary<string, AttachmentResult>>
    {
        private readonly string _documentId;
        private readonly IEnumerable<string> _names;
        private readonly AttachmentType _type;
        private readonly string _changeVector;

        public GetAttachmentsOperation(string documentId, IEnumerable<string> names, AttachmentType type, string changeVector)
        {
            _documentId = documentId;
            _names = names;
            _type = type;
            _changeVector = changeVector;
        }

        public RavenCommand<Dictionary<string, AttachmentResult>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetAttachmentsCommand(context, _documentId, _names, _type, _changeVector);
        }

        private class GetAttachmentsCommand : RavenCommand<Dictionary<string, AttachmentResult>>
        {
            private readonly JsonOperationContext _context;
            private readonly string _documentId;
            private readonly IEnumerable<string> _names;
            private readonly AttachmentType _type;
            private readonly string _changeVector;

            public GetAttachmentsCommand(JsonOperationContext context, string documentId, IEnumerable<string> names, AttachmentType type, string changeVector)
            {
                if (string.IsNullOrWhiteSpace(documentId))
                    throw new ArgumentNullException(nameof(documentId));

                if (type != AttachmentType.Document && changeVector == null)
                    throw new ArgumentNullException(nameof(changeVector), $"Change Vector cannot be null for attachment type {type}");

                _context = context;
                _documentId = documentId;
                _names = names ?? throw new ArgumentNullException(nameof(names));
                _type = type;
                _changeVector = changeVector;

                ResponseType = RavenCommandResponseType.Empty;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/attachments/list?id={Uri.EscapeDataString(_documentId)}";

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

                            writer.WriteArray("Names", _names);
                            writer.WriteComma();

                            writer.WritePropertyName("ChangeVector");
                            writer.WriteString(_changeVector);

                            writer.WriteEndObject();
                        }
                    })
                };

                return request;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                context.Reset();
                context.Renew();

                AttachmentsDetails attachmentsMetadata;
                AttachmentsStreamInfo streamInfo;
                var state = new JsonParserState();
                Stream stream = response.Content.ReadAsStreamAsync().Result;
                context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer);

                using (var parser = new UnmanagedJsonParser(context, state, "attachments/receive"))
                using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "attachments/list", parser, state))
                using (var peepingTomStream = new PeepingTomStream(stream, context))
                {
                    if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
                        throw new Exception("cannot parse stream");

                    if (state.CurrentTokenType != JsonParserToken.StartObject)
                        throw new Exception("got unexpected token");

                    await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);

                    BlittableJsonReaderObject data = builder.CreateReader();
                    attachmentsMetadata = JsonDeserializationClient.AttachmentAdvancedDetails(data);

                    builder.Reset();
                    buffer.Used = parser.BufferOffset;
                    buffer.Valid = parser.BufferSize;

                    streamInfo = new AttachmentsStreamInfo
                    {
                        AttachmentAdvancedDetails = attachmentsMetadata,
                        Buffer = buffer
                    };
                }

                Result = attachmentsMetadata.AttachmentsMetadata.ToDictionary(attachment => attachment.Name, attachment => new AttachmentResult
                {
                    Stream = new AttachmentsStream(stream, attachment.Name, attachment.Index, (int)attachment.Size, streamInfo),
                    Details = attachment
                });

                return ResponseDisposeHandling.Manually;
            }

            public override bool IsReadRequest => true;
        }
    }
}
