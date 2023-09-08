using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Test;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal sealed class AdminIndexHandlerProcessorForTestIndex : AbstractAdminIndexHandlerProcessorForTestIndex<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AdminIndexHandlerProcessorForTestIndex([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;
    
    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var testIndexParameters = await GetTestIndexParametersAsync(context);
            
            var testIndexDefinition = testIndexParameters.IndexDefinition;
            var query = testIndexParameters.Query;
            var queryParameters = testIndexParameters.QueryParameters;
            int maxDocumentsPerIndex = testIndexParameters.MaxDocumentsToProcess ?? 100;
            int waitForNonStaleResultsTimeoutInSec = testIndexParameters.WaitForNonStaleResultsTimeoutInSec ?? 15;

            const int documentsPerIndexUpperLimit = 10_000;
            const int documentsPerIndexLowerLimit = 1;
            
            const string defaultTestIndexName = "<TestIndexName>";

            if (testIndexParameters.IndexDefinition is null)
                throw new BadRequestException($"Index must have an {nameof(TestIndexParameters.IndexDefinition)} field");

            if (maxDocumentsPerIndex > documentsPerIndexUpperLimit || maxDocumentsPerIndex < documentsPerIndexLowerLimit)
                throw new BadRequestException($"Number of documents to process cannot be bigger than {documentsPerIndexUpperLimit} or less than {documentsPerIndexLowerLimit}.");

            if (testIndexDefinition.Type.IsJavaScript() == false)
            {
                // C# index without admin authorization
                if (HttpContext.Features.Get<IHttpAuthenticationFeature>() is RavenServer.AuthenticateConnection feature && feature.CanAccess(RequestHandler.Database.Name, requireAdmin: true, requireWrite: true) == false)
                    throw new UnauthorizedAccessException("Testing C# indexes requires admin privileges.");
            }

            var providedIndexName = testIndexDefinition.Name;

            if (string.IsNullOrEmpty(providedIndexName))
                providedIndexName = defaultTestIndexName;
            
            query ??= $"from index \"{providedIndexName}\"";
            
            var testIndexName = Guid.NewGuid().ToString("N");
            
            query = query.Replace(providedIndexName, testIndexName);
            
            testIndexDefinition.Name = testIndexName;
                
            var djv = new DynamicJsonValue() { [nameof(IndexQueryServerSide.Query)] = query, [nameof(IndexQueryServerSide.QueryParameters)] = queryParameters };

            var queryAsBlittable = context.ReadObject(djv, "test-index-query");

            using var tracker = new RequestTimeTracker(HttpContext, Logger, RequestHandler.Database.NotificationCenter, RequestHandler.Database.Configuration, "Query");
                
            var indexQueryServerSide = IndexQueryServerSide.Create(HttpContext, queryAsBlittable, RequestHandler.Database.QueryMetadataCache, tracker);

            if (indexQueryServerSide.Metadata.IndexName != testIndexName)
                throw new BadRequestException($"Expected '{testIndexName}' as index name in query, but could not find it.");
                
            using (var index = RequestHandler.Database.IndexStore.CreateTestIndexFromDefinition(testIndexDefinition, context.DocumentDatabase, context, maxDocumentsPerIndex))
            {
                index.Start();

                var timespanToWaitForProcessing = TimeSpan.FromSeconds(waitForNonStaleResultsTimeoutInSec);
                index.TestRun.WaitForProcessingOfSampleDocs(timespanToWaitForProcessing);
                    
                using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
                using (var queryContext = QueryOperationContext.Allocate(RequestHandler.Database))
                {
                    // we don't wait for non stale results because
                    // it's handled by WaitForProcessingOfSampleDocs
                    indexQueryServerSide.WaitForNonStaleResults = false;

                    var entries = await index.IndexEntries(indexQueryServerSide, queryContext, ignoreLimit: false, token);
                    var mapResults = index.TestRun.MapResults;
                    var reduceResults = index.TestRun.ReduceResults;
                    var queryResults = await index.Query(indexQueryServerSide, queryContext, token);
                    var hasDynamicFields = index.Definition.HasDynamicFields;

                    var result = new TestIndexResult()
                    {
                        IndexEntries = entries.Results,
                        QueryResults = queryResults.Results,
                        MapResults = mapResults,
                        HasDynamicFields = hasDynamicFields,
                        ReduceResults = reduceResults,
                        IsStale = queryResults.IsStale,
                        IndexType = testIndexDefinition.Type
                    };

                    await result.WriteTestIndexResultAsync(RequestHandler.ResponseBodyStream(), context);
                }
            }
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
