using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions
{
    /// <summary>
    /// Operation to enforce the current revisions configuration on all existing revisions.
    /// This applies the current revision configuration (rules), which are usually applied when a document is modified, 
    /// to all revisions at once.
    /// </summary>
    public sealed class EnforceRevisionsConfigurationOperation : IOperation<OperationIdResult>
    {
        private readonly Parameters _parameters;

        /// <summary>
        /// Parameters for the <see cref="EnforceRevisionsConfigurationOperation"/>, 
        /// allowing specification of whether to include force-created revisions and target specific collections.
        /// </summary>
        public sealed class Parameters : IRevisionsOperationParameters
        {
            /// <summary>
            /// Gets or sets a value indicating whether to include force-created revisions.
            /// For more information, visit <a href="https://ravendb.net/docs/article-page/6.2/csharp/document-extensions/revisions/overview#force-revision-creation">here</a>.
            /// </summary>
            public bool IncludeForceCreated { get; set; } = false;

            /// <summary>
            /// Gets or sets the collections to which the enforcement should apply. 
            /// If <c>null</c>, the operation will apply to all collections in the database.
            /// </summary>
            public string[] Collections { get; set; } = null;
        }

        /// <summary>
        /// Operation to enforce the current revisions configuration on all existing revisions.
        /// This applies the current revision configuration (rules), which are usually applied when a document is modified, 
        /// to all revisions at once.
        /// Initializes a new instance of <see cref="EnforceRevisionsConfigurationOperation"/> with default parameters.
        /// </summary>
        public EnforceRevisionsConfigurationOperation()
            : this(new Parameters())
        {

        }

        /// <summary>
        /// Operation to enforce the current revisions configuration on all existing revisions.
        /// This applies the current revision configuration (rules), which are usually applied when a document is modified,
        /// to all revisions at once.
        /// Initializes a new instance of <see cref="EnforceRevisionsConfigurationOperation"/> with the specified parameters.
        /// </summary>
        /// <param name="parameters">The parameters specifying whether to include force-created revisions and the target collections.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="parameters"/> is <c>null</c>.</exception>
        public EnforceRevisionsConfigurationOperation(Parameters parameters)
        {
            _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        }

        public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
        {
            return new EnforceRevisionsConfigurationCommand(_parameters, conventions);
        }

        internal sealed class EnforceRevisionsConfigurationCommand : RavenCommand<OperationIdResult>
        {
            private readonly Parameters _parameters;
            private readonly DocumentConventions _conventions;

            public EnforceRevisionsConfigurationCommand(Parameters parameters, DocumentConventions conventions)
            {
                _parameters = parameters;
                _conventions = conventions;
            }
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url)
                    .Append("/databases/")
                    .Append(node.Database)
                    .Append("/admin/revisions/config/enforce");

                url = pathBuilder.ToString();

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx);
                        await ctx.WriteAsync(stream, config).ConfigureAwait(false);
                    }, _conventions)
                };

                return request;
            }

            public override bool IsReadRequest => false;

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
