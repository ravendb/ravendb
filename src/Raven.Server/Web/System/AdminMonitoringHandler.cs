using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Monitoring;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Monitoring;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public sealed class AdminMonitoringHandler : ServerRequestHandler
    {
        [RavenAction("/admin/monitoring/v1/server", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringServer()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var provider = new MetricsProvider(Server);
            var result = provider.CollectServerMetrics();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }

        [RavenAction("/admin/monitoring/v1/databases", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringDatabases()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var databases = GetDatabases();

            var result = new DatabasesMetrics();

            result.PublicServerUrl = Server.Configuration.Core.PublicServerUrl?.UriValue;
            result.NodeTag = ServerStore.NodeTag;

            var provider = new MetricsProvider(Server);

            foreach (DocumentDatabase documentDatabase in databases)
            {
                var metrics = provider.CollectDatabaseMetrics(documentDatabase);
                result.Results.Add(metrics);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }

        private List<DocumentDatabase> GetDatabases()
        {
            var names = GetStringValuesQueryString("name", required: false);

            var databases = new List<DocumentDatabase>();
            var landlord = ServerStore.DatabasesLandlord;

            if (names.Count == 0)
            {
                foreach (var kvp in landlord.DatabasesCache)
                {
                    if (kvp.Value.IsCompletedSuccessfully == false)
                        continue;

                    databases.Add(kvp.Value.Result);
                }
            }
            else
            {
                foreach (string name in names)
                {
                    if (landlord.IsDatabaseLoaded(name))
                    {
                        var database = landlord.TryGetOrCreateResourceStore(name).Result;
                        databases.Add(database);
                    }
                }
            }

            return databases;
        }


        [RavenAction("/admin/monitoring/v1/indexes", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringIndexes()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var databases = GetDatabases();

            var result = new IndexesMetrics();

            result.PublicServerUrl = Server.Configuration.Core.PublicServerUrl?.UriValue;
            result.NodeTag = ServerStore.NodeTag;

            var provider = new MetricsProvider(Server);

            foreach (DocumentDatabase documentDatabase in databases)
            {
                var perDatabaseMetrics = new PerDatabaseIndexMetrics {DatabaseName = documentDatabase.Name};

                foreach (var index in documentDatabase.IndexStore.GetIndexes())
                {
                    var indexMetrics = provider.CollectIndexMetrics(index);
                    perDatabaseMetrics.Indexes.Add(indexMetrics);
                }

                result.Results.Add(perDatabaseMetrics);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }


        [RavenAction("/admin/monitoring/v1/collections", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringCollections()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var databases = GetDatabases();

            var result = new CollectionsMetrics();

            result.PublicServerUrl = Server.Configuration.Core.PublicServerUrl?.UriValue;
            result.NodeTag = ServerStore.NodeTag;

            foreach (DocumentDatabase documentDatabase in databases)
            {
                var perDatabaseMetrics = new PerDatabaseCollectionMetrics {DatabaseName = documentDatabase.Name};

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var collection in documentDatabase.DocumentsStorage.GetCollections(context))
                    {
                        var details = documentDatabase.DocumentsStorage.GetCollectionDetails(context, collection.Name);
                        perDatabaseMetrics.Collections.Add(new CollectionMetrics(details));
                    }
                }

                result.Results.Add(perDatabaseMetrics);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
        
        [RavenAction("/admin/monitoring/v1/etls", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringEtls()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var databases = GetDatabases();
            
            var result = new EtlsMetrics();

            result.PublicServerUrl = Server.Configuration.Core.PublicServerUrl?.UriValue;
            result.NodeTag = ServerStore.NodeTag;
            
            var provider = new MetricsProvider(Server);
            
            foreach (var documentDatabase in databases)
            {
                var perDatabaseMetrics = new PerDatabaseEtlMetrics { DatabaseName = documentDatabase.Name };

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var etl in documentDatabase.EtlLoader.GetEtlProcesses())
                    {
                        var etlMetrics = provider.CollectEtlMetrics(etl, documentDatabase.TaskErrorsStorage);
                        perDatabaseMetrics.Etls.Add(etlMetrics);
                    }
                }

                result.Results.Add(perDatabaseMetrics);
            }
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }

        [RavenAction("/admin/monitoring/v1/ai-tasks", "GET", AuthorizationStatus.Operator)]
        public async Task MonitoringAiTasks()
        {
            ServerStore.LicenseManager.AssertCanUseMonitoringEndpoints();

            var databases = GetDatabases();

            var result = new AiTasksMetrics();

            result.PublicServerUrl = Server.Configuration.Core.PublicServerUrl?.UriValue;
            result.NodeTag = ServerStore.NodeTag;

            var provider = new MetricsProvider(Server);

            foreach (var documentDatabase in databases)
            {
                var perDatabaseMetrics = new PerDatabaseAiTaskMetrics { DatabaseName = documentDatabase.Name };

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var aiTask in documentDatabase.EtlLoader.GetAiProcesses())
                    {
                        var aiTaskMetrics = provider.CollectAiTaskMetrics(aiTask, documentDatabase.TaskErrorsStorage);
                        perDatabaseMetrics.AiTasks.Add(aiTaskMetrics);
                    }
                }

                result.Results.Add(perDatabaseMetrics);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }
}
