using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Index = Raven.Server.Documents.Indexes.Index;

namespace SlowTests.Issues
{
    internal static class IndexDefinitionClusterStateAccess
    {
        // The cluster state of an index definition is internal to the client.
        private static readonly FieldInfo ClusterState = typeof(IndexDefinitionBase).GetField("ClusterState", BindingFlags.Instance | BindingFlags.NonPublic);

        public static string RenamedFrom(this IndexDefinitionBase definition) => (string)definition.ClusterStateField("RenamedFrom");

        public static object ClusterStateField(this IndexDefinitionBase definition, string name)
        {
            var clusterState = ClusterState.GetValue(definition);
            return clusterState.GetType().GetField(name).GetValue(clusterState);
        }

        public static void SetRenamedFrom(this IndexDefinitionBase definition, string renamedFrom)
        {
            var clusterState = Activator.CreateInstance(ClusterState.FieldType);
            clusterState.GetType().GetField("RenamedFrom").SetValue(clusterState, renamedFrom);
            ClusterState.SetValue(definition, clusterState);
        }
    }

    public class RavenDB_28080 : ClusterTestBase
    {
        public RavenDB_28080(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly DateTime CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RenameMovesTheDefinitionAndStartsARollingRename()
        {
            var record = CreateRecord();
            PutIndex(record, MapReduceDefinition("Orders/Totals"), etag: 7);
            var update = MapReduceDefinition("Orders/Totals");
            update.Priority = IndexPriority.High;
            PutIndex(record, update, etag: 8);

            var definition = record.Indexes["Orders/Totals"];
            Assert.Equal(7, definition.ReduceOutputIndex);
            var maps = definition.Maps.ToList();

            Rename(record, "Orders/Totals", "Orders/ByCompany", etag: 20);

            Assert.False(record.Indexes.ContainsKey("Orders/Totals"));
            var renamed = record.Indexes["Orders/ByCompany"];
            Assert.Equal("Orders/ByCompany", renamed.Name);
            Assert.Equal(maps, renamed.Maps.ToList());
            Assert.Equal(7, renamed.ReduceOutputIndex);
            Assert.Equal("Orders/Totals", renamed.RenamedFrom());
            Assert.Equal(20L, renamed.ClusterStateField("LastIndex"));
            Assert.Equal(20L, renamed.ClusterStateField("LastRollingDeploymentIndex"));

            Assert.False(record.IndexesHistory.ContainsKey("Orders/Totals"));
            var history = record.IndexesHistory["Orders/ByCompany"];
            Assert.Equal(3, history.Count);
            Assert.Equal("Renamed from 'Orders/Totals'", history[0].Source);
            Assert.Equal("Orders/ByCompany", history[0].Definition.Name);

            var rollingIndex = record.RollingIndexes["Orders/ByCompany"];
            Assert.Equal("Orders/Totals", rollingIndex.RenamedFrom);
            Assert.Equal(20, rollingIndex.RaftCommandIndex);
            Assert.Equal(RollingIndexState.Running, rollingIndex.ActiveDeployments["C"].State);
            Assert.Equal(RollingIndexState.Pending, rollingIndex.ActiveDeployments["A"].State);
            Assert.Equal(RollingIndexState.Pending, rollingIndex.ActiveDeployments["B"].State);
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RenameIsCompleteWhenAllNodesReportDone()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);
            Rename(record, "Users/ByName", "Users/ByFullName", etag: 20);

            // the finish of an older deployment, or of the index under its old name, does not change the rename
            new PutRollingIndexCommand("db", "Users/ByFullName", "C", CreatedAt, "1") { DeploymentRaftCommandIndex = 7 }.UpdateDatabaseRecord(record, 21);
            new PutRollingIndexCommand("db", "Users/ByName", "C", CreatedAt, "2") { DeploymentRaftCommandIndex = 20 }.UpdateDatabaseRecord(record, 22);
            Assert.Equal(RollingIndexState.Running, record.RollingIndexes["Users/ByFullName"].ActiveDeployments["C"].State);

            foreach (var node in new[] { "C", "B", "A" })
            {
                Assert.Equal(RollingIndexState.Running, record.RollingIndexes["Users/ByFullName"].ActiveDeployments[node].State);
                new PutRollingIndexCommand("db", "Users/ByFullName", node, CreatedAt, node) { DeploymentRaftCommandIndex = 20 }.UpdateDatabaseRecord(record, 23);
            }

            Assert.Empty(record.RollingIndexes);
            Assert.Equal("Users/ByName", record.Indexes["Users/ByFullName"].RenamedFrom());
            Assert.Equal(3, record.IndexesHistory["Users/ByFullName"][0].RollingDeployment.Count);

            // the old name is free after the rename is complete
            PutIndex(record, MapDefinition("Users/ByName"), etag: 30);
            Assert.True(record.Indexes.ContainsKey("Users/ByName"));
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RenameIsRefusedWhenTheRulesAreNotMet()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);
            PutIndex(record, MapDefinition("Orders/ByCompany"), etag: 8);
            record.AutoIndexes["Auto/Users/ByAge"] = new AutoIndexDefinition { Name = "Auto/Users/ByAge", Collection = "Users" };

            AssertRenameFails<IndexDoesNotExistException>(record, "Users/ByAge", "Users/ByYears");
            AssertRenameFails<InvalidOperationException>(record, "Auto/Users/ByAge", "Users/ByYears", "auto index");

            AssertRenameFails<ArgumentException>(record, "Users/ByName", "Auto/Users/ByName");
            AssertRenameFails<ArgumentException>(record, "Users/ByName", "ReplacementOf/Users/ByName");
            AssertRenameFails<ArgumentException>(record, "Users/ByName", "Users//ByName");

            AssertRenameFails<IndexAlreadyExistException>(record, "Users/ByName", "Orders/ByCompany");
            AssertRenameFails<IndexAlreadyExistException>(record, "Users/ByName", "orders/bycompany");
            AssertRenameFails<IndexAlreadyExistException>(record, "Users/ByName", "users/byname");
            AssertRenameFails<IndexAlreadyExistException>(record, "Users/ByName", "Orders_ByCompany");
            AssertRenameFails<IndexAlreadyExistException>(record, "Users/ByName", "Auto_Users_ByAge");

            foreach (var lockMode in new[] { IndexLockMode.LockedIgnore, IndexLockMode.LockedError })
            {
                record.Indexes["Users/ByName"].LockMode = lockMode;
                AssertRenameFails<InvalidOperationException>(record, "Users/ByName", "Users/ByFullName", "lock mode");
            }

            record.Indexes["Users/ByName"].LockMode = IndexLockMode.Unlock;

            record.RollingIndexes = new Dictionary<string, RollingIndex> { ["Users/ByName"] = new RollingIndex { RaftCommandIndex = 7 } };
            AssertRenameFails<InvalidOperationException>(record, "Users/ByName", "Users/ByFullName", "rolling deployment");
            record.RollingIndexes = null;

            // both names of a rename in progress are reserved, also for a rename back
            Rename(record, "Users/ByName", "Users/ByFullName", etag: 20);
            AssertRenameFails<InvalidOperationException>(record, "Users/ByFullName", "Users/ByName", "is in progress");
            AssertRenameFails<InvalidOperationException>(record, "Users/ByFullName", "Users/ByOtherName", "is in progress");
            AssertRenameFails<InvalidOperationException>(record, "Users/ByFullName", "users/byname", "is in progress");
            AssertRenameFails<InvalidOperationException>(record, "Users/ByName", "Users/ByOtherName", "is in progress");
            AssertRenameFails<InvalidOperationException>(record, "Orders/ByCompany", "Users/ByName", "is in progress");
            AssertRenameFails<InvalidOperationException>(record, "Orders/ByCompany", "Users/ByFullName", "is in progress");
            AssertRenameFails<IndexAlreadyExistException>(record, "Orders/ByCompany", "Users_ByName");

            var sharded = CreateRecord();
            sharded.Sharding = new ShardingConfiguration { Shards = new Dictionary<int, DatabaseTopology> { [0] = new DatabaseTopology() } };
            PutIndex(sharded, MapDefinition("Users/ByName"), etag: 7);
            AssertRenameFails<NotSupportedInShardingException>(sharded, "Users/ByName", "Users/ByFullName");
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void BothNamesAreReservedDuringTheRename()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);
            Rename(record, "Users/ByName", "Users/ByFullName", etag: 20);

            foreach (var name in new[] { "Users/ByName", "Users/ByFullName" })
            {
                AssertFails<InvalidOperationException>(() => PutIndex(record, MapDefinition(name), etag: 21), "is in progress");
                AssertFails<InvalidOperationException>(() =>
                {
                    var command = new PutIndexesCommand("db", "test", CreatedAt, "1", revisionsToKeep: 10, IndexDeploymentMode.Parallel, IndexDeploymentMode.Parallel);
                    command.Static.Add(MapDefinition(name));
                    command.UpdateDatabaseRecord(record, 21);
                }, "is in progress");
                AssertFails<InvalidOperationException>(() => new SetIndexLockCommand(name, IndexLockMode.LockedError, "db", "1").UpdateDatabaseRecord(record, 21), "is in progress");
                AssertFails<InvalidOperationException>(() => new SetIndexPriorityCommand(name, IndexPriority.High, "db", "1").UpdateDatabaseRecord(record, 21), "is in progress");
                AssertFails<InvalidOperationException>(() => new SetIndexStateCommand(name, IndexState.Disabled, "db", "1").UpdateDatabaseRecord(record, 21), "is in progress");
            }

            // a name that differs only in letter case
            foreach (var name in new[] { "users/byname", "users/byfullname" })
                AssertFails<IndexCreationException>(() => PutIndex(record, MapDefinition(name), etag: 21), "different casing");

            // the old name has the same directory as this name
            AssertFails<IndexCreationException>(() => PutIndex(record, MapDefinition("Users_ByName"), etag: 21), "directory name collision");

            Assert.Equal(IndexLockMode.Unlock, record.Indexes["Users/ByFullName"].LockMode ?? IndexLockMode.Unlock);
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void DeleteDuringTheRenameUsesTheNewName()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);
            Rename(record, "Users/ByName", "Users/ByFullName", etag: 20);

            AssertFails<InvalidOperationException>(() => new DeleteIndexCommand("Users/ByName", "db", "1").UpdateDatabaseRecord(record, 21),
                "is in progress. Use the new name 'Users/ByFullName'.");
            Assert.Single(record.Indexes);

            new DeleteIndexCommand("Users/ByFullName", "db", "1").UpdateDatabaseRecord(record, 21);

            Assert.Empty(record.Indexes);
            Assert.Empty(record.IndexesHistory);
            Assert.Empty(record.RollingIndexes);
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void DefinitionUpdateClearsThePreviousName()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);
            Rename(record, "Users/ByName", "Users/ByFullName", etag: 20);
            record.RollingIndexes.Clear(); // the rename is complete

            var update = MapDefinition("Users/ByFullName", map: "from u in docs.Users select new { u.Name, u.Age }");
            PutIndex(record, update, etag: 30);
            Assert.Null(record.Indexes["Users/ByFullName"].RenamedFrom());
            Assert.Equal("Renamed from 'Users/ByName'", record.IndexesHistory["Users/ByFullName"][1].Source);

            // a definition from the client can have a value that it read from another index
            var copy = MapDefinition("Users/Copy");
            copy.SetRenamedFrom("Users/ByName");
            PutIndex(record, copy, etag: 31);
            Assert.Null(record.Indexes["Users/Copy"].RenamedFrom());
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RenameSurvivesSerialization()
        {
            var record = CreateRecord();
            PutIndex(record, MapDefinition("Users/ByName"), etag: 7);

            var command = new RenameIndexCommand("db", "Users/ByName", "Users/ByFullName", CreatedAt, revisionsToKeep: 10, "1");

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var commandJson = context.ReadObject(command.ToJson(context), "command");
                var deserialized = (RenameIndexCommand)JsonDeserializationCluster.Commands[nameof(RenameIndexCommand)](commandJson);
                Assert.Equal("Users/ByName", deserialized.IndexName);
                Assert.Equal("Users/ByFullName", deserialized.NewIndexName);
                Assert.Equal(CreatedAt, deserialized.CreatedAt);
                Assert.Equal(10, deserialized.RevisionsToKeep);

                deserialized.UpdateDatabaseRecord(record, 20);

                var recordJson = DocumentConventions.DefaultForServer.Serialization.DefaultConverter.ToBlittable(record, context);
                var stored = JsonDeserializationCluster.DatabaseRecord(recordJson);

                Assert.Equal("Users/ByName", stored.Indexes["Users/ByFullName"].RenamedFrom());
                Assert.Equal("Users/ByName", stored.RollingIndexes["Users/ByFullName"].RenamedFrom);
                Assert.Equal(20, stored.RollingIndexes["Users/ByFullName"].RaftCommandIndex);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void MoveDirectoryRefusesToMoveADirectoryToItself()
        {
            var parent = NewDataPath(forceCreateDir: true);
            var directory = Path.Combine(parent, "Users_ByName");
            Directory.CreateDirectory(directory);
            var marker = Path.Combine(directory, "marker.txt");
            File.WriteAllText(marker, "data");

            foreach (var destination in new[] { directory, directory + Path.DirectorySeparatorChar })
            {
                Assert.Throws<InvalidOperationException>(() => IOExtensions.MoveDirectory(directory, destination));
                Assert.True(File.Exists(marker));
            }

            if (OperatingSystem.IsWindows() == false)
            {
                // a different path to the same directory
                var link = Path.Combine(parent, "UsersLink");
                Directory.CreateSymbolicLink(link, directory);
                Assert.Throws<InvalidOperationException>(() => IOExtensions.MoveDirectory(directory, link));
                Assert.Single(Directory.GetFiles(directory));
                Directory.Delete(link);
            }

            var otherCase = Path.Combine(parent, "users_byname");
            if (Directory.Exists(otherCase))
            {
                // a case-insensitive file system: the same directory
                Assert.Throws<InvalidOperationException>(() => IOExtensions.MoveDirectory(directory, otherCase));
                Assert.True(File.Exists(marker));
                Assert.Single(Directory.GetFiles(directory));
            }
            else
            {
                // a case-sensitive file system: a different directory, which the move replaces
                Directory.CreateDirectory(otherCase);
                File.WriteAllText(Path.Combine(otherCase, "other.txt"), "other");

                IOExtensions.MoveDirectory(directory, otherCase);

                Assert.False(Directory.Exists(directory));
                Assert.Equal(new[] { "marker.txt" }, Directory.GetFiles(otherCase).Select(Path.GetFileName));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void RenameOperationStartsTheRename()
        {
            using var store = GetDocumentStore();
            new UsersByName().Execute(store);

            store.Maintenance.Send(new RenameIndexOperation("UsersByName", "Users/ByName"));

            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            Assert.False(record.Indexes.ContainsKey("UsersByName"));
            Assert.Equal("UsersByName", record.Indexes["Users/ByName"].RenamedFrom());
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void RenameOperationIsRefusedWhenTheRulesAreNotMet()
        {
            using var store = GetDocumentStore();
            new UsersByName().Execute(store);
            store.Maintenance.Send(new PutIndexesOperation(MapDefinition("Users/ByAge")));

            Assert.Throws<IndexDoesNotExistException>(() => store.Maintenance.Send(new RenameIndexOperation("Users/ByEmail", "Users/ByMail")));
            Assert.Throws<IndexAlreadyExistException>(() => store.Maintenance.Send(new RenameIndexOperation("UsersByName", "Users/ByAge")));

            using var sharded = Sharding.GetDocumentStore();
            new UsersByName().Execute(sharded);
            var e = Assert.ThrowsAny<RavenException>(() => sharded.Maintenance.Send(new RenameIndexOperation("UsersByName", "Users/ByName")));
            Assert.Contains("does not support renaming an index", e.Message);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task RenameKeepsTheIndexData()
        {
            using var store = GetDocumentStore(new Options { RunInMemory = false });
            await StoreUsersAsync(store, 10);
            await new UsersByName().ExecuteAsync(store);
            await new UserCountsByLastName().ExecuteAsync(store);
            Indexes.WaitForIndexing(store);

            var database = await GetDatabase(store.Database);
            var index = database.IndexStore.GetIndex(nameof(UsersByName));
            var mapReduceIndex = database.IndexStore.GetIndex(nameof(UserCountsByLastName));
            var createdAt = index.GetStats().CreatedTimestamp;
            var outputIds = await GetOutputDocumentIdsAsync(store);
            Assert.NotEmpty(outputIds);

            await store.Maintenance.SendAsync(new RenameIndexOperation(nameof(UsersByName), "Users/ByName"));
            await store.Maintenance.SendAsync(new RenameIndexOperation(nameof(UserCountsByLastName), "Users/CountsByLastName"));
            await WaitForRenameToCompleteAsync(store, "Users/ByName");
            await WaitForRenameToCompleteAsync(store, "Users/CountsByLastName");

            Assert.Same(index, database.IndexStore.GetIndex("Users/ByName"));
            Assert.Same(mapReduceIndex, database.IndexStore.GetIndex("Users/CountsByLastName"));
            Assert.Null(database.IndexStore.GetIndex(nameof(UsersByName)));
            Assert.Equal("Users/ByName", index.Name);
            Assert.Equal(createdAt, index.GetStats().CreatedTimestamp);

            var storagePath = database.Configuration.Indexing.StoragePath;
            Assert.True(Directory.Exists(storagePath.Combine("Users_ByName").FullPath));
            Assert.False(Directory.Exists(storagePath.Combine(nameof(UsersByName)).FullPath));

            Indexes.WaitForIndexing(store);
            Assert.Equal(outputIds, await GetOutputDocumentIdsAsync(store));

            using (var session = store.OpenAsyncSession())
            {
                var users = await session.Query<User>("Users/ByName").Where(x => x.Name == "user/3").ToListAsync();
                Assert.Single(users);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Cluster)]
        [InlineData(IndexDeploymentMode.Parallel)]
        [InlineData(IndexDeploymentMode.Rolling)]
        public async Task NodesRenameOneAtATime(IndexDeploymentMode deploymentMode)
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            using var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3,
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexDeploymentMode)] = deploymentMode.ToString()
            });
            await StoreUsersAsync(store, 10);
            await new UsersByName().ExecuteAsync(store);
            await WaitForRenameToCompleteAsync(store, nameof(UsersByName)); // the rolling deployment of the index
            await Indexes.WaitForIndexingInTheClusterAsync(store);

            var originals = new Dictionary<string, Index>();
            var databases = new Dictionary<string, DocumentDatabase>();
            var reports = new BlockingCollection<string>();
            using var proceed = new SemaphoreSlim(0);

            foreach (var node in nodes)
            {
                var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var tag = node.ServerStore.NodeTag;
                databases[tag] = database;
                originals[tag] = database.IndexStore.GetIndex(nameof(UsersByName));
                database.IndexStore.ForTestingPurposesOnly().BeforeIndexRenameReport = _ =>
                {
                    reports.Add(tag);
                    proceed.Wait(database.DatabaseShutdown);
                };
            }

            await store.Maintenance.SendAsync(new RenameIndexOperation(nameof(UsersByName), "Users/ByName"));

            var renamed = new HashSet<string>();
            for (var i = 0; i < 3; i++)
            {
                Assert.True(reports.TryTake(out var tag, TimeSpan.FromSeconds(30)), $"No node reported after {string.Join(", ", renamed)}");
                renamed.Add(tag);

                var rollingIndex = (await GetRecordAsync(store)).RollingIndexes["Users/ByName"];
                Assert.Equal(RollingIndexState.Running, rollingIndex.ActiveDeployments[tag].State);
                Assert.Equal(1, rollingIndex.ActiveDeployments.Values.Count(x => x.State == RollingIndexState.Running));

                // a record change while other nodes wait for their turn
                if (i == 0)
                    await store.Maintenance.SendAsync(new PutIndexesOperation(MapDefinition("Users/ByAge", "from u in docs.Users select new { u.Age }")));

                foreach (var (nodeTag, database) in databases)
                {
                    var expectedName = renamed.Contains(nodeTag) ? "Users/ByName" : nameof(UsersByName);
                    Assert.Same(originals[nodeTag], database.IndexStore.GetIndex(expectedName));
                    Assert.Equal(expectedName, originals[nodeTag].Name);
                    Assert.False(originals[nodeTag].IsPending, nodeTag);
                }

                proceed.Release();
            }

            await WaitForRenameToCompleteAsync(store, "Users/ByName");

            foreach (var (nodeTag, database) in databases)
            {
                Assert.Same(originals[nodeTag], database.IndexStore.GetIndex("Users/ByName"));
                Assert.Null(database.IndexStore.GetIndex(nameof(UsersByName)));
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task RenameAlsoRenamesTheSideBySideIndex()
        {
            using var store = GetDocumentStore(new Options { RunInMemory = false });
            await StoreUsersAsync(store, 10);
            await new UsersByName().ExecuteAsync(store);
            Indexes.WaitForIndexing(store);

            var database = await GetDatabase(store.Database);
            var index = database.IndexStore.GetIndex(nameof(UsersByName));

            // the side-by-side index is created, but does not run
            database.IndexStore.StopIndexing();
            await store.Maintenance.SendAsync(new ResetIndexOperation(nameof(UsersByName), IndexResetMode.SideBySide));
            var replacement = database.IndexStore.GetIndex("ReplacementOf/" + nameof(UsersByName));
            Assert.NotNull(replacement);

            await store.Maintenance.SendAsync(new RenameIndexOperation(nameof(UsersByName), "Users/ByName"));
            await WaitForRenameToCompleteAsync(store, "Users/ByName");

            Assert.Same(index, database.IndexStore.GetIndex("Users/ByName"));
            Assert.Same(replacement, database.IndexStore.GetIndex("ReplacementOf/Users/ByName"));
            Assert.Null(database.IndexStore.GetIndex("ReplacementOf/" + nameof(UsersByName)));

            database.IndexStore.StartIndexing();

            Assert.True(await WaitForValueAsync(() => ReferenceEquals(database.IndexStore.GetIndex("Users/ByName"), replacement), true));
            Assert.Null(database.IndexStore.GetIndex("ReplacementOf/Users/ByName"));
            Indexes.WaitForIndexing(store);
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Cluster)]
        public async Task DeleteDuringTheRenameDeletesTheIndexOnAllNodes()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 3 });
            await StoreUsersAsync(store, 10);
            await new UsersByName().ExecuteAsync(store);
            await Indexes.WaitForIndexingInTheClusterAsync(store);

            var databases = new List<DocumentDatabase>();
            var reports = new BlockingCollection<string>();
            using var hold = new ManualResetEventSlim();

            foreach (var node in nodes)
            {
                var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                databases.Add(database);
                database.IndexStore.ForTestingPurposesOnly().BeforeIndexRenameReport = _ =>
                {
                    reports.Add(node.ServerStore.NodeTag);
                    hold.Wait(database.DatabaseShutdown);
                };
            }

            await store.Maintenance.SendAsync(new RenameIndexOperation(nameof(UsersByName), "Users/ByName"));
            Assert.True(reports.TryTake(out _, TimeSpan.FromSeconds(30)));

            // the old name is refused on every node
            var e = await Assert.ThrowsAnyAsync<RavenException>(() => store.Maintenance.SendAsync(new DeleteIndexOperation(nameof(UsersByName))));
            Assert.Contains("Use the new name 'Users/ByName'", e.Message);

            await store.Maintenance.SendAsync(new DeleteIndexOperation("Users/ByName"));
            hold.Set();

            var record = await GetRecordAsync(store);
            Assert.Empty(record.Indexes);
            Assert.False(record.RollingIndexes?.ContainsKey("Users/ByName") ?? false);

            foreach (var database in databases)
            {
                Assert.False(await WaitForValueAsync(() => database.IndexStore.GetIndexes().Any(), false));
            }
        }

        private static async Task StoreUsersAsync(IDocumentStore store, int count)
        {
            using var session = store.OpenAsyncSession();
            for (var i = 0; i < count; i++)
                await session.StoreAsync(new User { Name = "user/" + i, LastName = "last/" + (i % 3), Age = i });

            await session.SaveChangesAsync();
        }

        private static async Task<DatabaseRecordWithEtag> GetRecordAsync(IDocumentStore store)
        {
            return await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
        }

        private async Task WaitForRenameToCompleteAsync(IDocumentStore store, string name)
        {
            var complete = await WaitForValueAsync(async () =>
            {
                var record = await GetRecordAsync(store);
                return record.RollingIndexes == null || record.RollingIndexes.ContainsKey(name) == false;
            }, true, timeout: 30_000);

            Assert.True(complete, $"The rolling entry of '{name}' was not removed");
        }

        private static async Task<List<string>> GetOutputDocumentIdsAsync(IDocumentStore store)
        {
            using var session = store.OpenAsyncSession();
            var results = await session.Advanced.AsyncRawQuery<object>("from UserCounts").ToListAsync();
            return results.Select(x => session.Advanced.GetDocumentId(x)).Order().ToList();
        }

        private class UserCountsByLastName : AbstractIndexCreationTask<User, UserCountsByLastName.Result>
        {
            public class Result
            {
                public string LastName { get; set; }
                public int Count { get; set; }
            }

            public UserCountsByLastName()
            {
                Map = users => from user in users select new Result { LastName = user.LastName, Count = 1 };
                Reduce = results => from result in results group result by result.LastName into g select new Result { LastName = g.Key, Count = g.Sum(x => x.Count) };
                OutputReduceToCollection = "UserCounts";
            }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users select new { user.Name };
            }
        }

        private static DatabaseRecord CreateRecord()
        {
            return new DatabaseRecord("db")
            {
                Topology = new DatabaseTopology { Members = new List<string> { "A", "B", "C" } },
                // the same comparers as a record read from storage
                Indexes = new Dictionary<string, IndexDefinition>(),
                AutoIndexes = new Dictionary<string, AutoIndexDefinition>(),
                IndexesHistory = new Dictionary<string, List<IndexHistoryEntry>>(StringComparer.OrdinalIgnoreCase)
            };
        }

        private static IndexDefinition MapDefinition(string name, string map = "from u in docs.Users select new { u.Name }")
        {
            return new IndexDefinition { Name = name, Maps = new HashSet<string> { map } };
        }

        private static IndexDefinition MapReduceDefinition(string name)
        {
            return new IndexDefinition
            {
                Name = name,
                Maps = new HashSet<string> { "from o in docs.Orders select new { o.Company, Count = 1 }" },
                Reduce = "from r in results group r by r.Company into g select new { Company = g.Key, Count = g.Sum(x => x.Count) }",
                OutputReduceToCollection = "OrderTotals"
            };
        }

        private static void PutIndex(DatabaseRecord record, IndexDefinition definition, long etag)
        {
            new PutIndexCommand(definition, "db", "test", CreatedAt, "1", revisionsToKeep: 10, IndexDeploymentMode.Parallel).UpdateDatabaseRecord(record, etag);
        }

        private static void Rename(DatabaseRecord record, string name, string newName, long etag)
        {
            new RenameIndexCommand("db", name, newName, CreatedAt, revisionsToKeep: 10, "1").UpdateDatabaseRecord(record, etag);
        }

        private static void AssertRenameFails<T>(DatabaseRecord record, string name, string newName, string message = null) where T : Exception
        {
            AssertFails<T>(() => Rename(record, name, newName, etag: 100), message);
        }

        private static void AssertFails<T>(Action action, string message) where T : Exception
        {
            var e = Assert.ThrowsAny<T>(action);
            if (message != null)
                Assert.Contains(message, e.Message);
        }
    }
}
