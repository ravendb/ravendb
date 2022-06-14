using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class AbstractIndexHandlerProcessorForTerms<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractIndexHandlerProcessorForTerms([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<TermsQueryResultServerSide> GetTermsAsync(string indexName, string field, string fromValue, int pageSize, long? resultEtag, OperationCancelToken token);

        public override async ValueTask ExecuteAsync()
        {
            using (var token = RequestHandler.CreateTimeLimitedOperationToken())
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var field = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("field");
                var indexName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var fromValue = RequestHandler.GetStringQueryString("fromValue", required: false) ?? "";
                var pageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;
                var resultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

                var terms = await GetTermsAsync(indexName, field, fromValue, pageSize, resultEtag, token);

                if (terms.NotModified)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteTermsQueryResult(context, terms);
                }
            }
        }

        internal class GetIndexTermsCommand : RavenCommand<TermsQueryResultServerSide>
        {
            private GetTermsOperation.GetTermsCommand _cmd;

            public GetIndexTermsCommand(string indexName, string field, string fromValue, int? pageSize)
            {
                _cmd = new GetTermsOperation.GetTermsCommand(indexName, field, fromValue, pageSize);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                return _cmd.CreateRequest(ctx, node, out url);
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationServer.TermsQueryResult(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
