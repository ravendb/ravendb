using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using static Raven.Server.Utils.MetricCacher.Keys;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForPutConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _index;

        protected AbstractOngoingTasksHandlerProcessorForPutConnectionString([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            var connectionStringType = ConnectionString.GetConnectionStringType(configuration);
            var connectionString = GetConnectionString(configuration, connectionStringType);
            
            List<string> errors = new ();
            if (connectionString.Validate(ref errors) == false)
                throw new BadRequestException($"Invalid connection string configuration. Errors: {string.Join($"{Environment.NewLine}", errors)}");

            if (LoggingSource.AuditLog.IsInfoEnabled)
            {
                RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "PUT", $"Connection string '{connectionString.Name}'");
            }

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            SecurityClearanceValidator.AssertSecurityClearance(connectionString, feature?.Status);
        }
        
        private static ConnectionString GetConnectionString(BlittableJsonReaderObject readerObject, ConnectionStringType connectionStringType)
        {
            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    return JsonDeserializationCluster.RavenConnectionString(readerObject);
                case ConnectionStringType.Sql:
                    return JsonDeserializationCluster.SqlConnectionString(readerObject);
                case ConnectionStringType.Olap:
                    return JsonDeserializationCluster.OlapConnectionString(readerObject);
                case ConnectionStringType.ElasticSearch:
                    return JsonDeserializationCluster.ElasticSearchConnectionString(readerObject);
                case ConnectionStringType.Queue:
                    return JsonDeserializationCluster.QueueConnectionString(readerObject);
                case ConnectionStringType.None:
                default:
                    throw new ArgumentOutOfRangeException(nameof(connectionStringType), connectionStringType, "Unexpected connection string type.");
            }
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var res = await RequestHandler.ServerStore.PutConnectionString(context, RequestHandler.DatabaseName, configuration, raftRequestId);
            _index = res.Item1;
            return res;
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.PutConnectionStringDebugTag, _index, configuration);
            return ValueTask.CompletedTask;
        }
    }
}
