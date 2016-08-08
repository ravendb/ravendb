using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationLoader : BaseReplicationLoader
    {
        private const int MaxSupportedSqlReplication = int.MaxValue; // TODO: Maybe this should be 128, 1024 or configurable?

        private BlittableJsonReaderObject _connections;

        public Action<SqlReplicationStatistics> AfterReplicationCompleted;

        public SqlReplicationLoader(DocumentDatabase database)
            : base(database)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected override bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return
                systemDocumentKey.StartsWith(Constants.SqlReplication.SqlReplicationConfigurationPrefix,
                    StringComparison.OrdinalIgnoreCase) ||
                systemDocumentKey.Equals(Constants.SqlReplication.SqlReplicationConnections, StringComparison.OrdinalIgnoreCase);
        }

        protected override void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var sqlReplicationConnections = _database.DocumentsStorage.Get(context, Constants.SqlReplication.SqlReplicationConnections);
                if (sqlReplicationConnections != null)
                {
                    object connections;
                    if (sqlReplicationConnections.Data.TryGetMember("Connections", out connections))
                    {
                        _connections = connections as BlittableJsonReaderObject;
                    }
                }

                var documents = _database.DocumentsStorage.GetDocumentsStartingWith(context, Constants.SqlReplication.SqlReplicationConfigurationPrefix, null, null, 0, MaxSupportedSqlReplication);
                foreach (var document in documents)
                {
                    var configuration = JsonDeserializationServer.SqlReplicationConfiguration(document.Data);
                    var sqlReplication = new SqlReplication(_database, configuration);
                    Replications.Add(sqlReplication);
                    if (sqlReplication.ValidateName() == false ||
                        sqlReplication.PrepareSqlReplicationConfig(_connections) == false)
                        return;
                    sqlReplication.Start();
                }
            }
        }

        public DynamicJsonValue SimulateSqlReplicationSqlQueries(SimulateSqlReplication simulateSqlReplication, DocumentsOperationContext context)
        {
            try
            {
                var document = _database.DocumentsStorage.Get(context, simulateSqlReplication.DocumentId);
                var sqlReplication = new SqlReplication(_database, simulateSqlReplication.Configuration);

                var result = sqlReplication.ApplyConversionScript(new List<Document> { document }, context);

                if (sqlReplication.PrepareSqlReplicationConfig(_connections, false) == false)
                {
                    return new DynamicJsonValue
                    {
                        ["LastAlert"] = sqlReplication.Statistics.LastAlert,
                    };
                }

                return sqlReplication.Simulate(simulateSqlReplication, context, result);
            }
            catch (Exception e)
            {
                return new DynamicJsonValue
                {
                    ["LastAlert"] = new Alert
                    {
                        IsError = true,
                        CreatedAt = SystemTime.UtcNow,
                        Message = "Last SQL replication operation for " + simulateSqlReplication.Configuration.Name + " was failed",
                        Title = "SQL replication error",
                        Exception = e.ToString(),
                        UniqueKey = "Sql Replication Error: " + simulateSqlReplication.Configuration.Name
                    },
                };
            }
        }
    }
}