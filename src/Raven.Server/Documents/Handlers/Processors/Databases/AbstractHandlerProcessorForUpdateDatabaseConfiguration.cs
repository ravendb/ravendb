using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Databases;

internal abstract class AbstractHandlerProcessorForUpdateDatabaseConfiguration<T, TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where T : class 
{
    private readonly bool _isBlittable;

    protected AbstractHandlerProcessorForUpdateDatabaseConfiguration([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
        _isBlittable = typeof(T) == typeof(BlittableJsonReaderObject);
    }

    protected virtual HttpStatusCode GetResponseStatusCode() => HttpStatusCode.OK;

    protected virtual async ValueTask<T> GetConfigurationAsync(TransactionOperationContext context, string databaseName, AsyncBlittableJsonTextWriter writer)
    {
        if (_isBlittable)
        {
            var configurationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), GetType().Name);
            return configurationJson as T;
        }

        throw new InvalidOperationException($"In order to convert to '{typeof(T).Name}' please override this method.");
    }

    protected virtual void OnBeforeUpdateConfiguration(ref T configuration, JsonOperationContext context)
    {
    }

    protected abstract Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, T configuration, string raftRequestId);

    protected virtual void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, T configuration, long index)
    {
    }

    protected virtual async ValueTask AssertCanExecuteAsync(string databaseName)
    {
        var canAccessDatabase = await RequestHandler.CanAccessDatabaseAsync(databaseName, requireAdmin: true, requireWrite: true);
        if (canAccessDatabase == false)
            throw new AuthorizationException($"Cannot modify configuration of '{databaseName}' database due to insufficient privileges.");
    }

    protected virtual ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, string databaseName, T configuration, string raftRequestId)
    {
        return ValueTask.CompletedTask;
    }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var databaseName = RequestHandler.DatabaseName;

            await AssertCanExecuteAsync(databaseName);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var configuration = await GetConfigurationAsync(context, databaseName, writer);
                if (configuration == null)
                    return; // all validation should be handled internally

                if (ResourceNameValidator.IsValidResourceName(databaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                    throw new BadRequestException(errorMessage);

                await RequestHandler.ServerStore.EnsureNotPassiveAsync();

                OnBeforeUpdateConfiguration(ref configuration, context);

                var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
                var (index, _) = await OnUpdateConfiguration(context, databaseName, configuration, raftRequestId);

                await RequestHandler.WaitForIndexNotificationAsync(index);

                RequestHandler.HttpContext.Response.StatusCode = (int)GetResponseStatusCode();

                var json = new DynamicJsonValue
                {
                    ["RaftCommandIndex"] = index
                };

                OnBeforeResponseWrite(context, json, configuration, index);

                context.Write(writer, json);

                await OnAfterUpdateConfiguration(context, databaseName, configuration, raftRequestId);
            }
        }
    }
}
