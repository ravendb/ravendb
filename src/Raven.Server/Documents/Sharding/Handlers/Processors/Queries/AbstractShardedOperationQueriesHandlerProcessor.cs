﻿using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal abstract class AbstractShardedOperationQueriesHandlerProcessor : AbstractOperationQueriesHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    protected AbstractShardedOperationQueriesHandlerProcessor([NotNull] ShardedDatabaseRequestHandler requestHandler, QueryMetadataCache queryMetadataCache) : base(requestHandler, queryMetadataCache)
    {
    }

    protected override long GetNextOperationId()
    {
        return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
    }

    protected override IDisposable AllocateContextForAsyncOperation(out TransactionOperationContext asyncOperationContext)
    {
        return ContextPool.AllocateOperationContext(out asyncOperationContext);
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.DatabaseContext.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.DatabaseContext.Configuration;

    protected abstract (Func<JsonOperationContext, int, RavenCommand<OperationIdResult>> CommandFactory, OperationType Type) GetOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options);

    protected override void ScheduleOperation(TransactionOperationContext asyncOperationContext, IDisposable returnAsyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        if (query.Limit != null)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Minor, "We don't support operations with queries having 'limit' - RavenDB-18663");

            throw new NotSupportedInShardingException("Query with limit is not supported in patch / delete by query operation");
        }

        var token = RequestHandler.CreateTimeLimitedBackgroundOperationToken();

        var op = GetOperation(query, operationId, options);

        var task = RequestHandler.DatabaseContext.Operations
            .AddRemoteOperation<OperationIdResult, BulkOperationResult, BulkInsertProgress>(
                operationId,
                op.Type,
                GetOperationDescription(query),
                detailedDescription: GetDetailedDescription(query),
                op.CommandFactory,
                token);

        _ = task.ContinueWith(_ =>
        {
            using (returnAsyncOperationContext)
                token.Dispose();
        });
    }
}
