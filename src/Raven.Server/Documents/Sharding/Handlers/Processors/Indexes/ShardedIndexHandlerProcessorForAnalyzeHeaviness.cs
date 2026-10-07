using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal sealed class ShardedIndexHandlerProcessorForAnalyzeHeaviness : AbstractDatabaseHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForAnalyzeHeaviness([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
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

            HashSet<string> collections = IndexDefinitionHeavinessAnalyzer.ExtractCollectionsFromMaps(indexDefinition);
            IndexHeavinessGrade grade = IndexDefinitionHeavinessAnalyzer.ComputeStaticGrade(indexDefinition, collections);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                DynamicJsonValue djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(grade);
                writer.WriteObject(context.ReadObject(djv, "index/heaviness-grade"));
            }
        }
    }
}
