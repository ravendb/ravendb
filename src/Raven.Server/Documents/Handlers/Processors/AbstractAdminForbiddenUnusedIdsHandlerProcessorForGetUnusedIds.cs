using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors;

internal abstract class
    AbstractAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler,
        TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminForbiddenUnusedIdsHandlerProcessorForGetUnusedIds([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var database = RequestHandler.DatabaseName;

        await ServerStore.EnsureNotPassiveAsync();

        var forbiddenIds = new Dictionary<string, string>();
        HashSet<string> unusedIds;
        bool validateContent;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var json = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "unused-databases-ids"))
        {
            var parameters = JsonDeserializationServer.Parameters.GetForbiddenUnusedIdsParameters(json);
            unusedIds = parameters.DatabaseIds;
            validateContent = parameters.ValidateContent;
        }

        if (validateContent)
        {
            foreach (var id in unusedIds)
            {
                ValidateDatabaseIdContent(id, unusedIds, forbiddenIds);
            }
        }

        if (unusedIds.Count > 0)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan))
                await ValidateUnusedIdsOnAllNodesAsync(unusedIds, forbiddenIds, database, token.Token);
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
            writer.WriteObject(context.ReadObject(DynamicJsonValue.Convert(forbiddenIds), "forbiddenIds"));
    }

    protected abstract Task ValidateUnusedIdsOnAllNodesAsync(HashSet<string> unusedIds, Dictionary<string, string> forbiddenIds,
        string databaseName, CancellationToken token);


    private static unsafe void ValidateDatabaseIdContent(string id, HashSet<string> unusedIds, Dictionary<string, string> forbiddenIds)
    {
        const int fixedLength = StorageEnvironment.Base64IdLength + StorageEnvironment.Base64IdLength % 4;

        if (id is not { Length: StorageEnvironment.Base64IdLength })
        {
            forbiddenIds.Add(id, $"Database ID '{id}' isn't valid because its length ({id.Length}) isn't {StorageEnvironment.Base64IdLength}.");
            unusedIds.Remove(id);
            return;
        }

        Span<byte> bytes = stackalloc byte[fixedLength / 3 * 4];
        char* buffer = stackalloc char[fixedLength];
        fixed (char* str = id)
        {
            Buffer.MemoryCopy(str, buffer, 24 * sizeof(char), StorageEnvironment.Base64IdLength * sizeof(char));
            for (int i = StorageEnvironment.Base64IdLength; i < fixedLength; i++)
                buffer[i] = '=';

            if (Convert.TryFromBase64Chars(new ReadOnlySpan<char>(buffer, fixedLength), bytes, out _) == false)
            {
                forbiddenIds.Add(id, $"Database ID '{id}' isn't valid because it isn't Base64Id (it contains chars which cannot be in Base64String).");
                unusedIds.Remove(id);
            }
        }
    }
}
