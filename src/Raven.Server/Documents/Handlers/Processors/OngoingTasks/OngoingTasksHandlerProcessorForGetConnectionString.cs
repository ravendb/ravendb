using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetConnectionString<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        private readonly string _databaseName;

        public OngoingTasksHandlerProcessorForGetConnectionString([NotNull] TRequestHandler requestHandler,
            [NotNull] JsonContextPoolBase<TOperationContext> contextPool, [NotNull] string databaseName)
            : base(requestHandler, contextPool)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
        }

        private static (Dictionary<string, RavenConnectionString>, Dictionary<string, SqlConnectionString>, Dictionary<string, OlapConnectionString>, Dictionary<string, ElasticSearchConnectionString>)
          GetConnectionString(RawDatabaseRecord rawRecord, string connectionStringName, ConnectionStringType connectionStringType)
        {
            var ravenConnectionStrings = new Dictionary<string, RavenConnectionString>();
            var sqlConnectionStrings = new Dictionary<string, SqlConnectionString>();
            var olapConnectionStrings = new Dictionary<string, OlapConnectionString>();
            var elasticSearchConnectionStrings = new Dictionary<string, ElasticSearchConnectionString>();

            switch (connectionStringType)
            {
                case ConnectionStringType.Raven:
                    var recordRavenConnectionStrings = rawRecord.RavenConnectionStrings;
                    if (recordRavenConnectionStrings != null && recordRavenConnectionStrings.TryGetValue(connectionStringName, out var ravenConnectionString))
                    {
                        ravenConnectionStrings.TryAdd(connectionStringName, ravenConnectionString);
                    }

                    break;

                case ConnectionStringType.Sql:
                    var recordSqlConnectionStrings = rawRecord.SqlConnectionStrings;
                    if (recordSqlConnectionStrings != null && recordSqlConnectionStrings.TryGetValue(connectionStringName, out var sqlConnectionString))
                    {
                        sqlConnectionStrings.TryAdd(connectionStringName, sqlConnectionString);
                    }

                    break;

                case ConnectionStringType.Olap:
                    var recordOlapConnectionStrings = rawRecord.OlapConnectionString;
                    if (recordOlapConnectionStrings != null && recordOlapConnectionStrings.TryGetValue(connectionStringName, out var olapConnectionString))
                    {
                        olapConnectionStrings.TryAdd(connectionStringName, olapConnectionString);
                    }

                    break;

                case ConnectionStringType.ElasticSearch:
                    var recordElasticConnectionStrings = rawRecord.ElasticSearchConnectionStrings;
                    if (recordElasticConnectionStrings != null && recordElasticConnectionStrings.TryGetValue(connectionStringName, out var elasticConnectionString))
                    {
                        elasticSearchConnectionStrings.TryAdd(connectionStringName, elasticConnectionString);
                    }

                    break;

                default:
                    throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");
            }

            return (ravenConnectionStrings, sqlConnectionStrings, olapConnectionStrings, elasticSearchConnectionStrings);
        }

        public override async ValueTask ExecuteAsync()
        {
            if (ResourceNameValidator.IsValidResourceName(_databaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            if (await RequestHandler.CanAccessDatabaseAsync(_databaseName, requireAdmin: true, requireWrite: false) == false)
                return;

            var connectionStringName = RequestHandler.GetStringQueryString("connectionStringName", false);
            var type = RequestHandler.GetStringQueryString("type", false);

            await RequestHandler.ServerStore.EnsureNotPassiveAsync();
            RequestHandler.HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                Dictionary<string, RavenConnectionString> ravenConnectionStrings;
                Dictionary<string, SqlConnectionString> sqlConnectionStrings;
                Dictionary<string, OlapConnectionString> olapConnectionStrings;
                Dictionary<string, ElasticSearchConnectionString> elasticSearchConnectionStrings;

                using (context.OpenReadTransaction())
                using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, _databaseName))
                {
                    if (connectionStringName != null)
                    {
                        if (string.IsNullOrWhiteSpace(connectionStringName))
                            throw new ArgumentException($"connectionStringName {connectionStringName}' must have a non empty value");

                        if (Enum.TryParse<ConnectionStringType>(type, true, out var connectionStringType) == false)
                            throw new NotSupportedException($"Unknown connection string type: {connectionStringType}");


                        (ravenConnectionStrings, sqlConnectionStrings, olapConnectionStrings, elasticSearchConnectionStrings) = GetConnectionString(rawRecord, connectionStringName, connectionStringType);
                    }
                    else
                    {
                        ravenConnectionStrings = rawRecord.RavenConnectionStrings;
                        sqlConnectionStrings = rawRecord.SqlConnectionStrings;
                        olapConnectionStrings = rawRecord.OlapConnectionString;
                        elasticSearchConnectionStrings = rawRecord.ElasticSearchConnectionStrings;
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    var result = new GetConnectionStringsResult
                    {
                        RavenConnectionStrings = ravenConnectionStrings,
                        SqlConnectionStrings = sqlConnectionStrings,
                        OlapConnectionStrings = olapConnectionStrings,
                        ElasticSearchConnectionStrings = elasticSearchConnectionStrings
                    };
                    context.Write(writer, result.ToJson());
                }
            }
        }
    }
}
