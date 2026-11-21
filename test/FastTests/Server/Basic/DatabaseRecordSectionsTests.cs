using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Queries.Sorting;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Basic
{
    public class DatabaseRecordSectionsTests : NoDisposalNeeded
    {
        public DatabaseRecordSectionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void AllDatabaseRecordFieldsMustBeMappedToSection()
        {
            var fields = GetDatabaseRecordFields()
                .Select(x => x.Name)
                .ToHashSet();

            foreach (var field in fields)
            {
                Assert.True(DatabaseRecordSectionsExtensions.SectionByField.ContainsKey(field),
                    $"Field '{field}' of {nameof(DatabaseRecord)} must be mapped to a section in {nameof(DatabaseRecordSectionsExtensions)}.{nameof(DatabaseRecordSectionsExtensions.SectionByField)}, " +
                    $"and the features that depend on it must handle the changes of this section");
            }

            foreach (var field in DatabaseRecordSectionsExtensions.SectionByField.Keys)
                Assert.True(fields.Contains(field), $"Field '{field}' doesn't exist in {nameof(DatabaseRecord)}");
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void CanStampSectionsAndGetTheirVersion()
        {
            DatabaseRecordClusterState nullState = null;
            Assert.Equal(0, nullState.GetVersion(DatabaseRecordSections.All));

            var state = new DatabaseRecordClusterState();
            Assert.Equal(0, state.GetVersion(DatabaseRecordSections.All));

            state.Stamp(DatabaseRecordSections.Indexes, 10);
            Assert.Equal(10, state.LastIndexesIndex);
            Assert.Null(state.LastTopologyIndex);
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.Indexes));
            Assert.Equal(0, state.GetVersion(DatabaseRecordSections.Topology));
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.Indexes | DatabaseRecordSections.Topology));

            // configuration isn't tracked
            state.Stamp(DatabaseRecordSections.Configuration, 15);
            Assert.Equal(10, state.GetVersion(DatabaseRecordSections.All));

            state.Stamp(DatabaseRecordSections.All, 20);
            foreach (var field in typeof(DatabaseRecordClusterState).GetFields())
                Assert.Equal(20L, (long?)field.GetValue(state));
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void ConsumerShouldHandleOnlyChangesOfItsSections()
        {
            var consumer = new DatabaseRecordChangeConsumer("Test", DatabaseRecordSections.Indexes | DatabaseRecordSections.Topology);
            var record = new DatabaseRecord("Test");

            // nothing was handled yet, even if the record has no cluster state (e.g. created by an older version)
            Assert.True(consumer.ShouldProcess(record, forced: false, out var version));
            Assert.Equal(0, version);
            consumer.MarkProcessed(version);

            Assert.False(consumer.ShouldProcess(record, forced: false, out _));
            Assert.True(consumer.ShouldProcess(record, forced: true, out _));

            record.ClusterState = new DatabaseRecordClusterState();
            record.ClusterState.Stamp(DatabaseRecordSections.Etls | DatabaseRecordSections.PeriodicBackups, 5);
            Assert.False(consumer.ShouldProcess(record, forced: false, out _));

            record.ClusterState.Stamp(DatabaseRecordSections.Topology, 7);
            Assert.True(consumer.ShouldProcess(record, forced: false, out version));
            Assert.Equal(7, version);
            consumer.MarkProcessed(version);

            Assert.False(consumer.ShouldProcess(record, forced: false, out _));

            record.ClusterState.Stamp(DatabaseRecordSections.Indexes, 9);
            Assert.True(consumer.ShouldProcess(record, forced: false, out version));
            Assert.Equal(9, version);
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void CanDeserializeOnlySectionsHandledOnEveryChange()
        {
            var record = new DatabaseRecord("Test")
            {
                Topology = new DatabaseTopology { Members = { "A" } },
                Expiration = new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100 },
                Client = new ClientConfiguration { MaxNumberOfRequestsPerSession = 10 },
                Indexes = new Dictionary<string, IndexDefinition>
                {
                    ["Users/ByName"] = CreateIndexDefinition()
                },
                IndexesHistory = new Dictionary<string, List<IndexHistoryEntry>>
                {
                    ["Users/ByName"] = [new IndexHistoryEntry { Definition = CreateIndexDefinition(), Source = "Test" }]
                },
                Sorters = { ["MySorter"] = new SorterDefinition { Name = "MySorter", Code = "code" } },
                PeriodicBackups = { new PeriodicBackupConfiguration { TaskId = 1, Name = "Backup" } },
                ExternalReplications = { new ExternalReplication("ExternalDatabase", "ExternalConnection") },
                RavenConnectionStrings = { ["ExternalConnection"] = new RavenConnectionString { Name = "ExternalConnection", Database = "ExternalDatabase" } },
                RavenEtls = { new RavenEtlConfiguration { Name = "Etl", ConnectionStringName = "ExternalConnection" } },
                ClusterState = new DatabaseRecordClusterState { LastIndexesIndex = 5 }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var json = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(record, context);

                // internal field, read on every change
                json.Modifications = new DynamicJsonValue(json) { ["SupportedFeatures"] = new DynamicJsonArray { "Feature" } };
                json = context.ReadObject(json, "record");

                var full = JsonDeserializationCluster.DatabaseRecord(json);
                var partial = JsonDeserializationCluster.DatabaseRecordForEveryChange(json);
                var empty = new DatabaseRecord();

                foreach (var field in GetDatabaseRecordFields())
                {
                    var expected = DatabaseRecordSectionsExtensions.IsHandledOnEveryChange(field.Name) ? field.GetValue(full) : field.GetValue(empty);
                    Assert.True(JsonConvert.SerializeObject(expected) == JsonConvert.SerializeObject(field.GetValue(partial)), $"Unexpected value of '{field.Name}'");
                }

                Assert.Equal("Test", partial.DatabaseName);
                Assert.Equal(new[] { "A" }, partial.Topology.Members);
                Assert.Equal(100, partial.Expiration.DeleteFrequencyInSec);
                Assert.Equal(10, partial.Client.MaxNumberOfRequestsPerSession);
                Assert.Equal(5, partial.ClusterState.LastIndexesIndex);
                Assert.Equal(new[] { "Feature" }, (IReadOnlyList<string>)typeof(DatabaseRecord).GetField("SupportedFeatures", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(partial));

                Assert.Single(full.Indexes);
                Assert.Single(full.IndexesHistory);
                Assert.Single(full.Sorters);
                Assert.Single(full.PeriodicBackups);
                Assert.Single(full.ExternalReplications);
                Assert.Single(full.RavenConnectionStrings);
                Assert.Single(full.RavenEtls);

                Assert.Null(partial.Indexes);
                Assert.Null(partial.IndexesHistory);
                Assert.Empty(partial.Sorters);
                Assert.Empty(partial.PeriodicBackups);
                Assert.Empty(partial.ExternalReplications);
                Assert.Empty(partial.RavenConnectionStrings);
                Assert.Empty(partial.RavenEtls);
            }
        }

        private static IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition { Name = "Users/ByName", Maps = { "from u in docs.Users select new { u.Name }" } };
        }

        private static IEnumerable<FieldInfo> GetDatabaseRecordFields()
        {
            return typeof(DatabaseRecord).GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(x => x.IsPublic || x.GetCustomAttribute<ForceJsonSerializationAttribute>() != null);
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public void EveryTaskTypeMustBeMappedToSection()
        {
            foreach (var taskType in Enum.GetValues<OngoingTaskType>())
            {
                var sections = DatabaseRecordSectionsExtensions.ForTaskType(taskType);

                if (taskType == OngoingTaskType.Subscription)
                {
                    // subscriptions aren't part of the database record
                    Assert.Equal(DatabaseRecordSections.All, sections);
                    continue;
                }

                Assert.True(sections != DatabaseRecordSections.All, $"Task type '{taskType}' must be mapped to the section of the database record it is stored in");
            }
        }
    }
}
