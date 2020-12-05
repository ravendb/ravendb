using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Config.Categories;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class DatabasesDebugHandler : RequestHandler
    {
        [RavenAction("/admin/debug/databases/idle", "GET", AuthorizationStatus.Operator)]
        public async Task Idle()
        {
            //var name = GetStringQueryString("name", required: false);

            var maxTimeDatabaseCanBeIdle = ServerStore.Configuration.Databases.MaxIdleTime.AsTimeSpan;
            var results = new List<DynamicJsonValue>();

            foreach (var databaseKvp in ServerStore.DatabasesLandlord.LastRecentlyUsed.ForceEnumerateInThreadSafeManner())
            {
                var statistics = new IdleDatabaseStatistics
                {
                    Name = databaseKvp.Key.ToString()
                };

                statistics.CanCleanup = ServerStore.CanUnloadDatabase(databaseKvp.Key, databaseKvp.Value, maxTimeDatabaseCanBeIdle, statistics, out _);

                results.Add(statistics.ToJson());
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(DatabaseConfiguration.MaxIdleTime));
                writer.WriteString(maxTimeDatabaseCanBeIdle.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(DatabaseConfiguration.FrequencyToCheckForIdle));
                writer.WriteString(ServerStore.Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan.ToString());
                writer.WriteComma();

                writer.WriteArray("Results", results, context);

                writer.WriteEndObject();
            }
        }

        public class IdleDatabaseStatistics : IDynamicJson
        {
            public IdleDatabaseStatistics()
            {
                Explanations = new List<string>();
            }

            public string Name { get; set; }

            public DateTime LastRecentlyUsed { get; set; }

            public DateTime LastWork { get; set; }

            public bool IsLoaded { get; set; }

            public bool CanCleanup { get; set; }

            public bool RunInMemory { get; set; }

            public bool CanUnload { get; set; }

            public int NumberOfChangesApiConnections { get; set; }

            public bool HasActiveOperations { get; set; }

            public List<string> Explanations { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Name)] = Name,
                    [nameof(LastRecentlyUsed)] = LastRecentlyUsed,
                    [nameof(LastWork)] = LastWork,
                    [nameof(IsLoaded)] = IsLoaded,
                    [nameof(CanCleanup)] = CanCleanup,
                    [nameof(RunInMemory)] = RunInMemory,
                    [nameof(CanUnload)] = CanUnload,
                    [nameof(NumberOfChangesApiConnections)] = NumberOfChangesApiConnections,
                    [nameof(HasActiveOperations)] = HasActiveOperations,
                    [nameof(Explanations)] = Explanations,
                };
            }
        }
    }
}
