using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForAnalyzeHeaviness : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForAnalyzeHeaviness([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "index/definition"))
        {
            IndexDefinition indexDefinition = JsonDeserializationServer.IndexDefinition(json);

            if (indexDefinition == null || indexDefinition.Maps.Count == 0)
                throw new BadRequestException("Index definition must contain at least one map.");

            var collections = IndexDefinitionHeavinessAnalyzer.ExtractCollectionsFromMaps(indexDefinition);
            IndexHeavinessGrade staticGrade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(indexDefinition, collections);

            IndexHeavinessGrade grade = staticGrade;
            var db = RequestHandler.Database;
            if (db != null)
            {
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docsContext))
                using (docsContext.OpenReadTransaction())
                {
                    IndexDefinitionHeavinessAnalyzer.CollectionDataProvider provider = collectionName =>
                    {
                        if (string.Equals(collectionName, Raven.Client.Constants.Documents.Collections.AllDocumentsCollection, StringComparison.OrdinalIgnoreCase))
                        {
                            long totalCount = 0;
                            long totalSize = 0;
                            foreach (string name in db.DocumentsStorage.GetCollectionsNames(docsContext))
                            {
                                CollectionDetails details = db.DocumentsStorage.GetCollectionDetails(docsContext, name);
                                totalCount += details.CountOfDocuments;
                                totalSize += details.DocumentsSize.SizeInBytes;
                            }
                            return (totalCount, totalSize);
                        }

                        CollectionDetails col = db.DocumentsStorage.GetCollectionDetails(docsContext, collectionName);
                        return (col.CountOfDocuments, col.DocumentsSize.SizeInBytes);
                    };

                    grade = IndexDefinitionHeavinessAnalyzer.ComputeFullGrade(staticGrade, collections, stats: null, provider);
                }
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(grade);
                writer.WriteObject(context.ReadObject(djv, "index/heaviness-grade"));
            }
        }
    }
}
