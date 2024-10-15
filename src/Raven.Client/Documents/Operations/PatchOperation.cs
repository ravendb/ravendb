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
    /// <inheritdoc cref="PatchOperation"/>
    public sealed class PatchOperation<TEntity> : PatchOperation
    {
        /// <inheritdoc cref="PatchOperation.PatchOperation(string, string, PatchRequest, PatchRequest, bool)"/>
        public PatchOperation(string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing = null, bool skipPatchIfChangeVectorMismatch = false)
            : base(id, changeVector, patch, patchIfMissing, skipPatchIfChangeVectorMismatch)
        {
        }
    }

    /// <summary>
    /// A Patch operation used to perform updates on a specific document in the database
    /// </summary>
    public class PatchOperation : IOperation<PatchResult>
    {
        public sealed class Result<TEntity>
        {
            /// <summary>
            /// The status of the patch operation on the document.
            /// </summary>
            public PatchStatus Status { get; set; }

            /// <summary>
            /// The document after the patch operation has completed (document may have remained unchanged)
            /// </summary>
            public TEntity Document { get; set; }
        }

        private readonly string _id;
        private readonly string _changeVector;
        private readonly PatchRequest _patch;
        private readonly PatchRequest _patchIfMissing;
        private readonly bool _skipPatchIfChangeVectorMismatch;

        /// <summary>
        ///     Executes a patch operation on a document in the database.<br/>
        /// </summary>
        /// <param name="id">The id of the document on which to execute the <paramref name="patch"/> operation</param>
        /// <param name="changeVector">
        ///     Change vector of the document to be patched.<br/>
        ///     Used to verify that the document was not modified before the patch reached it. <br/> Can be null.
        /// </param>
        /// <param name="patch">
        ///     The patch request to perform the operation.
        /// </param>
        /// <param name="patchIfMissing">A patch request to perform if the document was not found</param>
        /// <param name="skipPatchIfChangeVectorMismatch">Whether to skip the execution of the patch if the document has been modified.</param>
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

        internal sealed class PatchCommand : RavenCommand<PatchResult>
        {
            private readonly DocumentConventions _conventions;
            private readonly string _id;
            private readonly string _changeVector;
            private readonly BlittableJsonReaderObject _patch;
            private readonly bool _skipPatchIfChangeVectorMismatch;
            private readonly bool _returnDebugInformation;
            private readonly bool _test;

            public PatchCommand(DocumentConventions conventions, JsonOperationContext context, string id, string changeVector, PatchRequest patch, PatchRequest patchIfMissing, bool skipPatchIfChangeVectorMismatch, bool returnDebugInformation, bool test)
            : this(conventions, id, changeVector, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(new
            {
                Patch = patch,
                PatchIfMissing = patchIfMissing
            }, context), skipPatchIfChangeVectorMismatch, returnDebugInformation, test)
            {
                if (patch == null)
                    throw new ArgumentNullException(nameof(patch));
                if (string.IsNullOrWhiteSpace(patch.Script))
                    throw new ArgumentNullException(nameof(patch.Script));
                if (patchIfMissing != null && string.IsNullOrWhiteSpace(patchIfMissing.Script))
                    throw new ArgumentNullException(nameof(patchIfMissing.Script));
            }

            internal PatchCommand(DocumentConventions conventions, string id, string changeVector, BlittableJsonReaderObject patch, bool skipPatchIfChangeVectorMismatch, bool returnDebugInformation, bool test)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _id = id ?? throw new ArgumentNullException(nameof(id));
                _changeVector = changeVector;
                _patch = patch ?? throw new ArgumentNullException(nameof(patch));
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
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _patch).ConfigureAwait(false), _conventions)
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
