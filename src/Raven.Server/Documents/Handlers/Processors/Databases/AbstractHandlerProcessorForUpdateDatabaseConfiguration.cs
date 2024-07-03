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
    protected long _index;

    protected AbstractHandlerProcessorForUpdateDatabaseConfiguration([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
        _isBlittable = typeof(T) == typeof(BlittableJsonReaderObject);
    }

    protected virtual bool RequireAdmin => true;

    protected virtual HttpStatusCode GetResponseStatusCode() => HttpStatusCode.OK;

    protected virtual async ValueTask<T> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
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

    protected abstract Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, T configuration, string raftRequestId);

    protected virtual void OnBeforeResponseWrite(TransactionOperationContext context, DynamicJsonValue responseJson, T configuration, long index)
    {
    }

    protected virtual async ValueTask AssertCanExecuteAsync()
    {
        var canAccessDatabase = await RequestHandler.CanAccessDatabaseAsync(RequestHandler.DatabaseName, requireAdmin: RequireAdmin, requireWrite: true);
        if (canAccessDatabase == false)
            throw new AuthorizationException($"Cannot modify configuration of '{RequestHandler.DatabaseName}' database due to insufficient privileges.");
    }

    protected virtual async ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, T configuration, string raftRequestId)
    {
        await ValueTask.CompletedTask;
    }

    public override async ValueTask ExecuteAsync()
    {
        using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            await AssertCanExecuteAsync();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                var configuration = await GetConfigurationAsync(context, writer);
                if (configuration == null)
                    return; // all validation should be handled internally

                await UpdateConfigurationAsync(context, writer, configuration);
            }
        }
    }

    protected async ValueTask UpdateConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer, T configuration = null)
    {
        if (ResourceNameValidator.IsValidResourceName(RequestHandler.DatabaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
            throw new BadRequestException(errorMessage);

        await RequestHandler.ServerStore.EnsureNotPassiveAsync();

        OnBeforeUpdateConfiguration(ref configuration, context);

        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
        (_index, _) = await OnUpdateConfiguration(context, configuration, raftRequestId);

        await RequestHandler.WaitForIndexNotificationAsync(_index);

        RequestHandler.HttpContext.Response.StatusCode = (int)GetResponseStatusCode();

        var json = new DynamicJsonValue
        {
            ["RaftCommandIndex"] = _index
        };

        OnBeforeResponseWrite(context, json, configuration, _index);

        context.Write(writer, json);

        await OnAfterUpdateConfiguration(context, configuration, raftRequestId);
    }
}
