using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes
{
    internal class IndexHandlerProcessorForTerms : AbstractIndexHandlerProcessorForTerms<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public IndexHandlerProcessorForTerms([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override ValueTask<TermsQueryResultServerSide> GetTermsAsync(string indexName, string field, string fromValue, int pageSize, long? resultEtag)
        {
            using (var token = RequestHandler.CreateTimeLimitedOperationToken())
            using (var context = QueryOperationContext.Allocate(RequestHandler.Database))
            {
                var name = GetIndexNameFromCollectionAndField(field) ?? indexName;

                var result = RequestHandler.Database.QueryRunner.ExecuteGetTermsQuery(name, field, fromValue, resultEtag, RequestHandler.GetPageSize(), context, token, out var index);

                if (result.NotModified == false)
                {
                    HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);
                    if (field.EndsWith("__minX") ||
                        field.EndsWith("__minY") ||
                        field.EndsWith("__maxX") ||
                        field.EndsWith("__maxY"))
                    {
                        if (index.Definition.IndexFields != null &&
                            index.Definition.IndexFields.TryGetValue(field.Substring(0, field.Length - 6), out var indexField) == true)
                        {
                            if (indexField.Spatial?.Strategy == Client.Documents.Indexes.Spatial.SpatialSearchStrategy.BoundingBox)
                            {
                                // Term-values for 'Spatial Index Fields' with 'BoundingBox' are encoded in Lucene as 'prefixCoded bytes'
                                // Need to convert to numbers for the Studio
                                var readableTerms = new SortedSet<string>();
                                foreach (var item in result.Terms)
                                {
                                    var num = Lucene.Net.Util.NumericUtils.PrefixCodedToDouble(item);
                                    readableTerms.Add(NumberUtil.NumberToString(num));
                                }

                                result.Terms = readableTerms.ToList();
                            }
                        }
                    }
                }

                return ValueTask.FromResult(result);
            }
        }

        private string GetIndexNameFromCollectionAndField(string field)
        {
            var collection = RequestHandler.GetStringQueryString("collection", false);
            if (string.IsNullOrEmpty(collection))
                return null;
            var query = new IndexQueryServerSide(new QueryMetadata($"from {collection} select {field}", null, 0));
            var dynamicQueryToIndex = new DynamicQueryToIndexMatcher(RequestHandler.Database.IndexStore);
            var match = dynamicQueryToIndex.Match(DynamicQueryMapping.Create(query));
            if (match.MatchType == DynamicQueryMatchType.Complete ||
                match.MatchType == DynamicQueryMatchType.CompleteButIdle)
                return match.IndexName;
            throw new IndexDoesNotExistException($"There is no index to answer the following query: from {collection} select {field}");
        }
    }
}
