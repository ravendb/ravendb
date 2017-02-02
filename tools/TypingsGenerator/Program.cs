using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Data;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Client.Smuggler;
using Raven.Json.Linq;
using Raven.NewClient.Client.Data.Collections;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Versioning;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.PeriodicExport;
using Raven.Server.Web.System;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Server;
using Sparrow;
using Sparrow.Json;
using TypeScripter;
using TypeScripter.TypeScript;
using Voron.Data.BTrees;
using Voron.Debugging;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace TypingsGenerator
{
    public class Program
    {

        public const string TargetDirectory = "../../src/Raven.Studio/typings/server/";
        public static void Main(string[] args)
        {
            Directory.CreateDirectory(TargetDirectory);

            var scripter = new Scripter()
                .UsingFormatter(new TsFormatter
                {
                    EnumsAsString = true
                });

            scripter
                .WithTypeMapping(TsPrimitive.String, typeof(Guid))
                .WithTypeMapping(TsPrimitive.String, typeof(TimeSpan))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(HashSet<>))
                .WithTypeMapping(new TsInterface(new TsName("Array")), typeof(List<>))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJObject))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJValue))
                .WithTypeMapping(TsPrimitive.Any, typeof(TreePage))
                .WithTypeMapping(TsPrimitive.String, typeof(DateTime))
                .WithTypeMapping(new TsArray(TsPrimitive.Any, 1), typeof(RavenJArray))
                .WithTypeMapping(TsPrimitive.Any, typeof(RavenJToken))
                .WithTypeMapping(TsPrimitive.Any, typeof(BlittableJsonReaderObject));

            scripter = ConfigureTypes(scripter);
            Directory.Delete(TargetDirectory, true);
            Directory.CreateDirectory(TargetDirectory);
            scripter
                .SaveToDirectory(TargetDirectory);
        }

        private static Scripter ConfigureTypes(Scripter scripter)
        {
            var ignoredTypes = new HashSet<Type>
            {
                typeof(IEquatable<>)
            };

            scripter.UsingTypeFilter(type => ignoredTypes.Contains(type) == false);
            scripter.UsingTypeReader(new TypeReaderWithIgnoreMethods());
            scripter.AddType(typeof(CollectionStatistics));

            scripter.AddType(typeof(DatabaseDocument));
            scripter.AddType(typeof(DatabaseStatistics));
            scripter.AddType(typeof(IndexDefinition));

            // notifications
            scripter.AddType(typeof(AlertRaised));
            scripter.AddType(typeof(NotificationUpdated));
            scripter.AddType(typeof(OperationChanged));
            scripter.AddType(typeof(ResourceChanged));
            scripter.AddType(typeof(PerformanceHint));

            // changes
            scripter.AddType(typeof(OperationStatusChanged));
            scripter.AddType(typeof(DeterminateProgress));
            scripter.AddType(typeof(IndeterminateProgress));
            scripter.AddType(typeof(OperationExceptionResult));
            scripter.AddType(typeof(DocumentChange));
            scripter.AddType(typeof(IndexChange));
            scripter.AddType(typeof(TransformerChange));
            scripter.AddType(typeof(DatabaseOperations.Operation));
            
            // indexes
            scripter.AddType(typeof(IndexStats));
            scripter.AddType(typeof(IndexingStatus));
            scripter.AddType(typeof(IndexPerformanceStats));
            scripter.AddType(typeof(IndexDefinition));
            scripter.AddType(typeof(TermsQueryResult));

            // query 
            scripter.AddType(typeof(QueryResult<>));

            // transformers
            scripter.AddType(typeof(TransformerDefinition));

            // patch
            scripter.AddType(typeof(PatchRequest));

            scripter.AddType(typeof(ResourcesInfo));

            // smuggler
            scripter.AddType(typeof(DatabaseSmugglerOptions));

            // versioning
            scripter.AddType(typeof(VersioningConfiguration));

            // replication 
            scripter.AddType(typeof(ReplicationDocument<>));

            // sql replication 
            scripter.AddType(typeof(SqlConnections));
            scripter.AddType(typeof(SqlReplicationConfiguration));
            scripter.AddType(typeof(SqlReplicationStatistics));
            scripter.AddType(typeof(SimulateSqlReplication));

            // periodic export
            scripter.AddType(typeof(PeriodicExportConfiguration));

            // storage report
            scripter.AddType(typeof(StorageReport));
            scripter.AddType(typeof(DetailedStorageReport));

            // map reduce visualizer
            scripter.AddType(typeof(ReduceTree));

            // license 
            scripter.AddType(typeof(License));
            scripter.AddType(typeof(UserRegistrationInfo));
            scripter.AddType(typeof(LicenseStatus));

            // database admin
            scripter.AddType(typeof(ResourceDeleteResult));

            // io metrics stats
            scripter.AddType(typeof(IOMetricsHistoryStats));
            scripter.AddType(typeof(IOMetricsRecentStats));
            scripter.AddType(typeof(IOMetricsFileStats));
            scripter.AddType(typeof(IOMetricsEnvironment));
            scripter.AddType(typeof(IOMetricsResponse));
            scripter.AddType(typeof(FileStatus));
            scripter.AddType(typeof(IoMetrics.MeterType));

            return scripter;
        }
    }
}
