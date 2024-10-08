using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions;

/// <summary>
/// Operation to adopt orphaned revisions, which are revisions that belong to a deleted document but are not shown 
/// in the revisions bin, due to the absence of a "Delete Revision".
/// </summary>
public sealed class AdoptOrphanedRevisionsOperation : IOperation<OperationIdResult>
{
    private readonly Parameters _parameters;

    /// <summary>
    /// Parameters for the <see cref="AdoptOrphanedRevisionsOperation"/>, specifying which collections 
    /// to adopt orphaned revisions from.
    /// </summary>
    public sealed class Parameters : IRevisionsOperationParameters
    {
        /// <summary>
        /// Gets or sets the collections from which orphaned revisions will be adopted.
        /// If <c>null</c>, the operation will apply to all collections.
        /// </summary>
        public string[] Collections { get; set; } = null;
    }

    public AdoptOrphanedRevisionsOperation()
        : this(new Parameters())
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="AdoptOrphanedRevisionsOperation"/> with the specified parameters.
    /// Operation to adopt orphaned revisions, which are revisions that belong to a deleted document but are not shown 
    /// in the revisions bin, due to the absence of a "Delete Revision".
    /// </summary>
    /// <param name="parameters">The parameters specifying the collections for which orphaned revisions will be adopted.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="parameters"/> is <c>null</c>.</exception>
    public AdoptOrphanedRevisionsOperation(Parameters parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
    }

    public RavenCommand<OperationIdResult> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context, HttpCache cache)
    {
        return new AdoptOrphanedRevisionsCommand(_parameters, conventions);
    }

    internal sealed class AdoptOrphanedRevisionsCommand : RavenCommand<OperationIdResult>
    {
        private readonly Parameters _parameters;
        private readonly DocumentConventions _conventions;

        public AdoptOrphanedRevisionsCommand(Parameters parameters, DocumentConventions conventions)
        {
            _parameters = parameters;
            _conventions = conventions;
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var pathBuilder = new StringBuilder(node.Url)
                .Append("/databases/")
                .Append(node.Database)
                .Append("/admin/revisions/orphaned/adopt");

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

