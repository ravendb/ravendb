using System;
using System.Net.Http;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases.Documents
{
    public class PatchOperation : IOperation<PatchResult>
    {
        public class Result<TEntity>
        {
            public PatchStatus Status { get; set; }

            public TEntity Document { get; set; }
        }

        private readonly string _id;
        private readonly long? _etag;
        private readonly PatchRequest _patch;
        private readonly PatchRequest _patchIfMissing;
        private readonly bool _skipPatchIfEtagMismatch;
        private readonly bool _returnDebugInformation;

        public PatchOperation(string id, long? etag, PatchRequest patch, PatchRequest patchIfMissing = null, bool skipPatchIfEtagMismatch = false, bool returnDebugInformation = false)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (patch == null)
                throw new ArgumentNullException(nameof(patch));
            if (string.IsNullOrWhiteSpace(patch.Script))
                throw new ArgumentNullException(nameof(patch.Script));
            if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                throw new ArgumentNullException(nameof(patchIfMissing.Script));

            _id = id;
            _etag = etag;
            _patch = patch;
            _patchIfMissing = patchIfMissing;
            _skipPatchIfEtagMismatch = skipPatchIfEtagMismatch;
            _returnDebugInformation = returnDebugInformation;
        }

        public RavenCommand<PatchResult> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new PatchCommand(conventions, context, _id, _etag, _patch, _patchIfMissing, _skipPatchIfEtagMismatch, _returnDebugInformation);
        }

        public class PatchCommand : RavenCommand<PatchResult>
        {
            private readonly DocumentConvention _conventions;
            private readonly JsonOperationContext _context;
            private readonly string _id;
            private readonly long? _etag;
            private readonly BlittableJsonReaderObject _patch;
            private readonly bool _skipPatchIfEtagMismatch;
            private readonly bool _returnDebugInformation;

            public PatchCommand(DocumentConvention conventions, JsonOperationContext context, string id, long? etag, PatchRequest patch, PatchRequest patchIfMissing, bool skipPatchIfEtagMismatch, bool returnDebugInformation)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (id == null)
                    throw new ArgumentNullException(nameof(id));
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));
                if (string.IsNullOrWhiteSpace(patch.Script))
                    throw new ArgumentNullException(nameof(patch.Script));
                if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                    throw new ArgumentNullException(nameof(patchIfMissing.Script));

                _conventions = conventions;
                _context = context;
                _id = id;
                _etag = etag;
                _patch = new EntityToBlittable(null).ConvertEntityToBlittable(new
                {
                    Patch = patch,
                    PatchIfMissing = patchIfMissing
                }, conventions, context);
                _skipPatchIfEtagMismatch = skipPatchIfEtagMismatch;
                _returnDebugInformation = returnDebugInformation;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/docs?id={Uri.EscapeUriString(_id)}";
                if (_skipPatchIfEtagMismatch)
                    url += "&skipPatchIfEtagMismatch=true";
                if (_returnDebugInformation)
                    url += "&debug=true";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(stream =>
                    {
                        _context.Write(stream, _patch);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.PatchResult(response);
            }
        }
    }
}