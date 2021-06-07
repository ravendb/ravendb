using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class PatchOperation<TEntity> : PatchOperation
    {
        public PatchOperation(string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing = null, bool skipPatchIfChangeVectorMismatch = false)
            : base(id, changeVector, patch, patchIfMissing, skipPatchIfChangeVectorMismatch)
        {
        }
    }

    public class PatchOperation : IOperation<PatchResult>
    {
        public class Result<TEntity>
        {
            public PatchStatus Status { get; set; }

            public TEntity Document { get; set; }
        }

        private readonly string _id;
        private readonly string _changeVector;
        private readonly PatchRequest _patch;
        private readonly PatchRequest _patchIfMissing;
        private readonly bool _skipPatchIfChangeVectorMismatch;

        public PatchOperation(string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing = null, bool skipPatchIfChangeVectorMismatch = false)
        {
            if (patch == null)
                throw new ArgumentNullException(nameof(patch));
            if (string.IsNullOrWhiteSpace(patch.Script))
                throw new ArgumentNullException(nameof(patch.Script));
            if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                throw new ArgumentNullException(nameof(patchIfMissing.Script));

            _id = id ?? throw new ArgumentNullException(nameof(id));
            _changeVector = changeVector;
            _patch = patch;
            _patchIfMissing = patchIfMissing;
            _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
        }

        public RavenCommand<PatchResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new PatchCommand(conventions, context, _id, _changeVector, _patch, _patchIfMissing, _skipPatchIfChangeVectorMismatch, returnDebugInformation: false, test: false);
        }

        public RavenCommand<PatchResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache, bool returnDebugInformation, bool test)
        {
            return new PatchCommand(conventions, context, _id, _changeVector, _patch, _patchIfMissing, _skipPatchIfChangeVectorMismatch, returnDebugInformation, test);
        }

        internal class PatchCommand : RavenCommand<PatchResult>
        {
            private readonly string _id;
            private readonly string _changeVector;
            private readonly BlittableJsonReaderObject _patch;
            private readonly bool _skipPatchIfChangeVectorMismatch;
            private readonly bool _returnDebugInformation;
            private readonly bool _test;

            public PatchCommand(DocumentConventions conventions, JsonOperationContext context, string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing, bool skipPatchIfChangeVectorMismatch, bool returnDebugInformation, bool test)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));
                if (string.IsNullOrWhiteSpace(patch.Script))
                    throw new ArgumentNullException(nameof(patch.Script));
                if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                    throw new ArgumentNullException(nameof(patchIfMissing.Script));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                _id = id ?? throw new ArgumentNullException(nameof(id));
                _changeVector = changeVector;
                _patch = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new
                {
                    Patch = patch,
                    PatchIfMissing = patchIfMissing
                }, context);
                _skipPatchIfChangeVectorMismatch = skipPatchIfChangeVectorMismatch;
                _returnDebugInformation = returnDebugInformation;
                _test = test;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/docs?id={Uri.EscapeDataString(_id)}";
                if (_skipPatchIfChangeVectorMismatch)
                    url += "&skipPatchIfChangeVectorMismatch=true";
                if (_returnDebugInformation)
                    url += "&debug=true";
                if (_test)
                    url += "&test=true";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Patch,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _patch).ConfigureAwait(false))
                };
                AddChangeVectorIfNotNull(_changeVector, request);
                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;
                if (fromCache) // we should never cache the response here, but keeping it anyway
                {
                    // we have to clone the response here because  otherwise the cached item might be freed while
                    // we are still looking at this result, so we clone it to the side
                    response = response.Clone(context);
                }
                Result = JsonDeserializationClient.PatchResult(response);
            }
        }
    }
}
