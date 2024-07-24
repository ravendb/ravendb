using System.Collections.Generic;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Attachments.Retired
{
    public sealed class GetRetiredAttachmentsOperation : IOperation<IEnumerator<AttachmentEnumeratorResult>>
    {
        private readonly IEnumerable<AttachmentRequest> _attachments;

        public GetRetiredAttachmentsOperation(IEnumerable<AttachmentRequest> attachments)
        {
            _attachments = attachments;
        }

        public RavenCommand<IEnumerator<AttachmentEnumeratorResult>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new GetRetiredAttachmentsCommand(conventions, context, _attachments);
           // return new GetRetiredAttachmentsCommand(conventions, context, _attachments, AttachmentType.Document);
        }
        internal sealed class GetRetiredAttachmentsCommand : GetAttachmentsOperation.GetAttachmentsCommand
        {
            public GetRetiredAttachmentsCommand(DocumentConventions conventions, JsonOperationContext context, IEnumerable<AttachmentRequest> attachments) : base(conventions, context, attachments, AttachmentType.Document)
            {
            }

            protected override string GetUrl(ServerNode node)
            {
                return $"{node.Url}/databases/{node.Database}/attachments/retire/bulk";
            }
        }
        //internal sealed class GetRetiredAttachmentsCommand : RavenCommand<IEnumerator<AttachmentEnumeratorResult>>
        //{
        //    private readonly DocumentConventions _conventions;
        //    private readonly JsonOperationContext _context;
        //    private readonly AttachmentType _type;
        //    internal IEnumerable<AttachmentRequest> Attachments { get; }
        //    internal List<AttachmentDetails> AttachmentsMetadata { get; } = new List<AttachmentDetails>();

        //    public GetRetiredAttachmentsCommand(DocumentConventions conventions, JsonOperationContext context, IEnumerable<AttachmentRequest> attachments, AttachmentType type)
        //    {
        //        _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
        //        _context = context;
        //        _type = type;
        //        Attachments = attachments;
        //        ResponseType = RavenCommandResponseType.Empty;
        //    }
        //    public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        //    {
        //        url = $"{node.Url}/databases/{node.Database}/attachments/retire/bulk";

        //        var request = new HttpRequestMessage
        //        {
        //            Method = HttpMethods.Post,
        //            Content = new BlittableJsonContent(async stream =>
        //            {
        //                await using (var writer = new AsyncBlittableJsonTextWriter(_context, stream))
        //                {
        //                    writer.WriteStartObject();

        //                    writer.WritePropertyName(nameof(AttachmentType));
        //                    writer.WriteString(_type.ToString());
        //                    writer.WriteComma();

        //                    writer.WritePropertyName(nameof(Attachments));

        //                    writer.WriteStartArray();
        //                    var first = true;
        //                    foreach (var attachment in Attachments)
        //                    {
        //                        if (first == false)
        //                            writer.WriteComma();
        //                        first = false;

        //                        writer.WriteStartObject();
        //                        writer.WritePropertyName(nameof(AttachmentRequest.DocumentId));
        //                        writer.WriteString(attachment.DocumentId);
        //                        writer.WriteComma();
        //                        writer.WritePropertyName(nameof(AttachmentRequest.Name));
        //                        writer.WriteString(attachment.Name);
        //                        writer.WriteEndObject();
        //                    }
        //                    writer.WriteEndArray();

        //                    writer.WriteEndObject();
        //                }
        //            }, _conventions)
        //        };

        //        return request;
        //    }

        //    public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
        //    {
        //        var state = new JsonParserState();
        //        Stream stream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);

        //        using (context.GetMemoryBuffer(out var buffer))
        //        using (var parser = new UnmanagedJsonParser(context, state, "attachments/receive"))
        //        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "attachments/list", parser, state))
        //        using (var peepingTomStream = new PeepingTomStream(stream, context))
        //        {
        //            if (await UnmanagedJsonParserHelper.ReadAsync(peepingTomStream, parser, state, buffer).ConfigureAwait(false) == false)
        //                throw new Exception("cannot parse stream");

        //            if (state.CurrentTokenType != JsonParserToken.StartObject)
        //                throw new Exception($"Expected token {nameof(JsonParserToken.StartObject)}, but got {nameof(state.CurrentTokenType)}.");

        //            await UnmanagedJsonParserHelper.ReadObjectAsync(builder, peepingTomStream, parser, buffer).ConfigureAwait(false);
        //            using (var data = builder.CreateReader())
        //            {
        //                if (data.TryGetMember(nameof(AttachmentsMetadata), out object obj) && obj is BlittableJsonReaderArray bjra)
        //                {
        //                    foreach (BlittableJsonReaderObject e in bjra)
        //                    {
        //                        var cur = JsonDeserializationClient.AttachmentDetails(e);
        //                        AttachmentsMetadata.Add(cur);
        //                    }
        //                }
        //            }

        //            var bufferSize = parser.BufferSize - parser.BufferOffset;
        //            var copy = ArrayPool<byte>.Shared.Rent(bufferSize);
        //            var copyMemory = new Memory<byte>(copy);
        //            buffer.Memory.Memory.Slice(parser.BufferOffset, bufferSize).CopyTo(copyMemory);

        //            Result = Iterate(stream, copy, bufferSize).GetEnumerator();
        //        }

        //        return ResponseDisposeHandling.Manually;
        //    }

        //    private IEnumerable<AttachmentEnumeratorResult> Iterate(Stream stream, byte[] copy, int bufferSize)
        //    {
        //        LimitedStream prev = null;

        //        using (var cs = new ConcatStream(new ConcatStream.RentedBuffer
        //        {
        //            Buffer = copy,
        //            Count = bufferSize,
        //            Offset = 0
        //        }, stream))
        //        {
        //            long position = 0;

        //            foreach (var attachment in AttachmentsMetadata)
        //            {
        //                prev?.Dispose();

        //                prev = new LimitedStream(cs, attachment.Size, position, prev?.OverallRead ?? 0);
        //                position += attachment.Size;

        //                yield return new AttachmentEnumeratorResult(attachment, prev);
        //            }

        //            // mark the last attachment as disposed
        //            prev?.Dispose();
        //        }
        //    }

        //    public override bool IsReadRequest => true;
        //}
    }
}
