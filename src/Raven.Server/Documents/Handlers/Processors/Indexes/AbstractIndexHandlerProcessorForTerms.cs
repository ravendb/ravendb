using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal abstract class AbstractIndexHandlerProcessorForTerms<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractIndexHandlerProcessorForTerms([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask<TermsQueryResultServerSide> GetTerms(TransactionOperationContext context, string indexName, string field, string fromValue, int pageSize);

        public override async ValueTask ExecuteAsync()
        {
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var field = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("field");
                var indexName = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var fromValue = RequestHandler.GetStringQueryString("fromValue", required: false) ?? "";
                var pageSize = RequestHandler.GetIntValueQueryString("pageSize", required: false) ?? int.MaxValue;

                var terms = await GetTerms(context, indexName, field, fromValue, pageSize);

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

        public class GetTermsCommand : RavenCommand<TermsQueryResultServerSide>
        {
            private readonly string _indexName;
            private readonly string _field;
            [CanBeNull]
            private readonly string _fromValue;
            private readonly int? _pageSize;

            public GetTermsCommand(string indexName, string field, string fromValue, int? pageSize)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _field = field ?? throw new ArgumentNullException(nameof(field));
                _fromValue = fromValue;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var fromValue = _fromValue != null ? Uri.EscapeDataString(_fromValue) : "";
                url = $"{node.Url}/databases/{node.Database}/indexes/terms?name={Uri.EscapeDataString(_indexName)}&field={Uri.EscapeDataString(_field)}&fromValue={fromValue}";

                if (_pageSize.HasValue)
                    url += $"&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
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
