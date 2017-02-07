using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Replication.Messages;
using Raven.NewClient.Client.Exceptions;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Operations.Databases.Transformers;
using Raven.NewClient.Operations.Databases.Indexes;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationIndexesAndTransformers : ReplicationTestsBase
    {
        private class User
        {
            public string Name { get; set; }
            public int Age { get; set; }
            public DateTime Birthday { get; set; }
        }

        private class UserByNameAndBirthday : AbstractIndexCreationTask<User>
        {
            public UserByNameAndBirthday()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name,
                                   user.Birthday
                               };
            }
        }


        private class UserByNameIndex : AbstractIndexCreationTask<User>
        {
            private readonly string _indexName;

            public override string IndexName =>
                string.IsNullOrEmpty(_indexName) ? base.IndexName : _indexName;


            public UserByNameIndex(string name = null)
            {
                _indexName = name;
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        private class UserByAgeIndex : AbstractIndexCreationTask<User>
        {
            private readonly string _indexName;

            public override string IndexName =>
                string.IsNullOrEmpty(_indexName) ? base.IndexName : _indexName;

            public UserByAgeIndex(string name = null)
            {
                _indexName = name;
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        private class UsernameToUpperTransformer : AbstractTransformerCreationTask<User>
        {
            private readonly string _transformerName;

            public override string TransformerName =>
                string.IsNullOrEmpty(_transformerName) ? base.TransformerName : _transformerName;

            public UsernameToUpperTransformer(string transformerName = null)
            {
                _transformerName = transformerName;
                TransformResults = users => from user in users
                                            select new
                                            {
                                                Name = user.Name.ToUpper(),
                                                user.Age,
                                                user.Birthday
                                            };
            }
        }

        private class UsernameToLowerTransformer : AbstractTransformerCreationTask<User>
        {
            private readonly string _transformerName;

            public override string TransformerName =>
                string.IsNullOrEmpty(_transformerName) ? base.TransformerName : _transformerName;

            public UsernameToLowerTransformer(string transformerName = null)
            {
                _transformerName = transformerName;
                TransformResults = users => from user in users
                                            select new
                                            {
                                                Name = user.Name.ToLower(),
                                                user.Age,
                                                user.Birthday
                                            };
            }
        }

        [Fact]
        public void DeleteConflictsFor_should_delete_all_conflict_records()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(store);

                var databaseStoreTask =
                    Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
                databaseStoreTask.Wait();
                var databaseStore = databaseStoreTask.Result;

                var definitionJson = new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                };

                var definitionJson2 = new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                };

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                using (var definintion = context.ReadObject(definitionJson, string.Empty))
                using (var definintion2 = context.ReadObject(definitionJson2, string.Empty))
                {
                    databaseStore.IndexMetadataPersistence.AddConflict(
                        context,
                        tx.InnerTransaction,
                        userByAge.IndexName,
                        IndexEntryType.Index,
                        new ChangeVectorEntry[0], definintion);

                    databaseStore.IndexMetadataPersistence.AddConflict(
                        context,
                        tx.InnerTransaction,
                        userByAge.IndexName,
                        IndexEntryType.Index,
                        new ChangeVectorEntry[0], definintion2);

                    tx.Commit();
                }


                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var changeVectors = databaseStore.IndexMetadataPersistence.DeleteConflictsFor(tx.InnerTransaction, context, userByAge.IndexName);
                    tx.Commit();

                    Assert.Equal(2, changeVectors.Count);
                }

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    Assert.Empty(databaseStore.IndexMetadataPersistence.GetConflictsFor(tx.InnerTransaction, context, userByAge.IndexName, 0, 1024));
                }

            }
        }

        [Fact]
        public void Adding_conflict_should_set_the_original_metadata_as_conflicted()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(store);

                var databaseStoreTask = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
                databaseStoreTask.Wait();
                var databaseStore = databaseStoreTask.Result;

                IndexesEtagsStorage.IndexEntryMetadata metadata;
                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);

                Assert.False(metadata.IsConflicted);

                var definitionJson = new DynamicJsonValue
                {
                    ["Foo"] = "Bar"
                };

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                using (var definintion = context.ReadObject(definitionJson, string.Empty))
                {
                    databaseStore.IndexMetadataPersistence.AddConflict(
                        context,
                        tx.InnerTransaction,
                        userByAge.IndexName,
                        IndexEntryType.Index,
                        new ChangeVectorEntry[0], definintion);

                    tx.Commit();
                }

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);

                Assert.True(metadata.IsConflicted);
            }
        }

        [Fact]
        public async Task Setting_conflicted_should_work()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                IndexesEtagsStorage.IndexEntryMetadata metadata;
                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);

                Assert.NotNull(metadata); //sanity check
                Assert.False(metadata.IsConflicted);

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    Assert.True(databaseStore.IndexMetadataPersistence.TrySetConflictedByName(context,
                        tx.InnerTransaction,
                        userByAge.IndexName));

                    tx.Commit();
                }

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);

                Assert.True(metadata.IsConflicted);
            }
        }

        [Fact]
        public async Task Deleting_indexes_should_delete_relevant_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex();
                userByName.Execute(store);
                userByAge.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                store.Admin.Send(new DeleteIndexOperation(userByName.IndexName));

                IndexesEtagsStorage.IndexEntryMetadata metadata;
                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);
                Assert.NotNull(metadata);

                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByName.IndexName);
                Assert.Null(metadata);

                store.Admin.Send(new DeleteIndexOperation(userByAge.IndexName));
                using (var tx = context.OpenReadTransaction())
                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);
                Assert.Null(metadata);

            }
        }

        [Fact]
        public void Conflicting_indexes_should_record_conflicts_in_metadata()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex(userByAge.IndexName);
                userByAge.Execute(nodeA);

                SetupReplication(nodeA, nodeB);

                userByName.Execute(nodeA);

            }
        }

        [Fact]
        public void Can_replicate_index()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destination);

                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 1)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(1, destIndexNames.Length);
                Assert.Equal(userByAge.IndexName, destIndexNames.First());
            }
        }

        [Fact]
        public void Can_replicate_multiple_indexes()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                SetupReplication(source, destination);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));
            }
        }

        [Fact]
        public void Can_replicate_multiple_indexes_and_multiple_transformers()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                userByAge.Execute(source);

                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var userByName = new UserByNameIndex();
                userByName.Execute(source);

                var usernameToLowerTransformer = new UsernameToLowerTransformer();
                usernameToLowerTransformer.Execute(source);

                SetupReplication(source, destination);

                var sw = Stopwatch.StartNew();
                var destIndexNames = new string[0];
                var destTransformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && destIndexNames.Length != 2)
                    destIndexNames = destination.Admin.Send(new GetIndexNamesOperation(0, 1024));

                sw.Restart();
                while (sw.ElapsedMilliseconds < timeout && destTransformerNames.Length != 2)
                    destTransformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(destIndexNames); //precaution
                Assert.Equal(2, destIndexNames.Length);
                Assert.True(destIndexNames.Contains(userByAge.IndexName));
                Assert.True(destIndexNames.Contains(userByName.IndexName));

                Assert.NotNull(destTransformerNames); //precaution
                Assert.Equal(2, destTransformerNames.Length);
                Assert.True(destTransformerNames.Contains(usernameToUpperTransformer.TransformerName));
                Assert.True(destTransformerNames.Contains(usernameToLowerTransformer.TransformerName));
            }
        }

        [Fact]
        public void Can_replicate_transformer()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destination);

                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var sw = Stopwatch.StartNew();
                var transformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && transformerNames.Length != 1)
                    transformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(transformerNames); //precaution
                Assert.Equal(1, transformerNames.Length);
                Assert.Equal(usernameToUpperTransformer.TransformerName, transformerNames.First());
            }
        }

        [Fact]
        public void Can_replicate_multiple_transformers()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                var usernameToUpperTransformer = new UsernameToUpperTransformer();
                usernameToUpperTransformer.Execute(source);

                var usernameToLowerTransformer = new UsernameToLowerTransformer();
                usernameToLowerTransformer.Execute(source);

                SetupReplication(source, destination);

                var sw = Stopwatch.StartNew();
                var transformerNames = new string[0];
                var timeout = Debugger.IsAttached ? 60 * 1000000 : 3000;
                while (sw.ElapsedMilliseconds < timeout && transformerNames.Length != 2)
                    transformerNames = destination.Admin.Send(new GetTransformerNamesOperation(0, 1024));

                Assert.NotNull(transformerNames); //precaution
                Assert.Equal(2, transformerNames.Length);
                Assert.True(transformerNames.Contains(usernameToUpperTransformer.TransformerName));
                Assert.True(transformerNames.Contains(usernameToLowerTransformer.TransformerName));
            }
        }

        [Fact]
        public async Task PurgeTombstonesFrom_should_work_properly()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex();
                var userByNameAndBirthday = new UserByNameAndBirthday();
                userByName.Execute(store);
                userByAge.Execute(store);
                userByNameAndBirthday.Execute(store);

                store.Admin.Send(new DeleteIndexOperation(userByName.IndexName));
                store.Admin.Send(new DeleteIndexOperation(userByNameAndBirthday.IndexName));

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenWriteTransaction())
                {
                    databaseStore.IndexMetadataPersistence.PurgeTombstonesFrom(tx.InnerTransaction, context, 0, 1024);
                    tx.Commit();
                }

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var metadataCollection = databaseStore.IndexMetadataPersistence.GetAfter(tx.InnerTransaction, context, 0, 0, 1024);
                    Assert.Equal(1, metadataCollection.Count);
                    Assert.Equal(userByAge.IndexName.ToLower(), metadataCollection[0].Name);
                }
            }
        }

        [Fact]
        public async Task GetAfter_should_work_properly()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex();
                var userByNameAndBirthday = new UserByNameAndBirthday();
                userByName.Execute(store);
                userByAge.Execute(store);
                userByNameAndBirthday.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                List<IndexesEtagsStorage.IndexEntryMetadata> metadataItems;
                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(tx.InnerTransaction, context, 0, 0, 1024);
                    Assert.Equal(3, metadataItems.Count);

                    metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(tx.InnerTransaction, context, 3, 0, 1024);
                    Assert.Equal(1, metadataItems.Count);
                }

                //this one was created last, so it has the largest etag
                Assert.Equal(userByNameAndBirthday.IndexName.ToLower(), metadataItems[0].Name);

                store.Admin.Send(new DeleteIndexOperation(userByName.IndexName));
                store.Admin.Send(new DeleteIndexOperation(userByNameAndBirthday.IndexName));

                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    metadataItems = databaseStore.IndexMetadataPersistence.GetAfter(tx.InnerTransaction, context, 0, 0, 1024);
                    Assert.Equal(3, metadataItems.Count); //together with tombstones
                    Assert.Equal(2, metadataItems.Count(item => item.Id == -1));
                }
            }
        }

        [Fact]
        public async Task Deleting_indexes_should_write_tombstones()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex();
                userByName.Execute(store);
                userByAge.Execute(store);

                store.Admin.Send(new DeleteIndexOperation(userByName.IndexName));
                store.Admin.Send(new DeleteIndexOperation(userByAge.IndexName));

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())

                {
                    var metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName,
                        returnNullIfTombstone: false);
                    Assert.NotNull(metadata);
                    Assert.Equal(-1, metadata.Id);
                    Assert.Equal(userByAge.IndexName.ToLower(), metadata.Name);


                    metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByName.IndexName,
                        returnNullIfTombstone: false);
                    Assert.NotNull(metadata);
                    Assert.Equal(-1, metadata.Id);
                    Assert.Equal(userByName.IndexName.ToLower(), metadata.Name);
                }

            }
        }

        [Fact]
        public async Task Creating_indexes_should_create_relevant_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var userByAge = new UserByAgeIndex();
                var userByName = new UserByNameIndex();
                userByName.Execute(store);
                userByAge.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var metadataByName =
                        databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByName.IndexName);
                    Assert.NotNull(metadataByName);

                    var serversideIndexMetadata = databaseStore.IndexStore.GetIndex(userByName.IndexName);
                    Assert.Equal(serversideIndexMetadata.IndexId, metadataByName.Id);

                    var metadataByAge =
                        databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAge.IndexName);
                    Assert.NotNull(metadataByAge);

                    serversideIndexMetadata = databaseStore.IndexStore.GetIndex(userByAge.IndexName);
                    Assert.Equal(serversideIndexMetadata.IndexId, metadataByAge.Id);
                }
            }
        }

        //An index can't be named with the same name as transformer or vice versa
        [Fact]
        public void Index_and_transformer_metadata_storage_should_enforce_name_uniqueness_for_writing_index_then_transformer()
        {
            using (var store = GetDocumentStore())
            {
                var userByNameIndex = new UserByNameIndex("FooBar");
                userByNameIndex.Execute(store);

                var usernameToUpperTransformer = new UsernameToUpperTransformer("FooBar");
                Assert.Throws<RavenException>(() => usernameToUpperTransformer.Execute(store));
            }
        }

        [Fact]
        public void Index_and_transformer_metadata_storage_should_enforce_name_uniqueness_for_writing_transformer_then_index()
        {
            using (var store = GetDocumentStore())
            {
                var usernameToUpperTransformer = new UsernameToUpperTransformer("FooBar");
                usernameToUpperTransformer.Execute(store);

                var userByNameIndex = new UserByNameIndex("FooBar");
                Assert.Throws<RavenException>(() => userByNameIndex.Execute(store));
            }
        }

        [Fact]
        public async Task New_index_will_overwrite_transformer_tombstones()
        {
            using (var store = GetDocumentStore())
            {
                const string name = "FooBar";
                var usernameToUpperTransformer = new UsernameToUpperTransformer(name);
                usernameToUpperTransformer.Execute(store);

                store.Admin.Send(new DeleteTransformerOperation(name));

                var userByNameIndex = new UserByNameIndex(name);
                userByNameIndex.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())

                {
                    var metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, name);
                    Assert.Equal(1, metadata.ChangeVector.Length); //sanity check

                    /*
                     transformers and etags share the same tombstone,
                     so if transformer created, then deleted, then index created under the same name as transformer, the change vector
                     which represents history of the object will be preserved
                    */
                    Assert.Equal(3, metadata.ChangeVector[0].Etag);
                }
            }
        }

        [Fact]
        public async Task New_transformer_will_overwrite_index_tombstones()
        {
            using (var store = GetDocumentStore())
            {
                const string name = "FooBar";
                var userByNameIndex = new UserByNameIndex(name);
                userByNameIndex.Execute(store);

                store.Admin.Send(new DeleteIndexOperation(name));

                var usernameToUpperTransformer = new UsernameToUpperTransformer(name);
                usernameToUpperTransformer.Execute(store);

                var databaseStore =
                    await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                TransactionOperationContext context;
                using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())

                {
                    var metadata = databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, name);
                    Assert.Equal(1, metadata.ChangeVector.Length); //sanity check

                    /*
                     transformers and etags share the same tombstone,
                     so if transformer created, then deleted, then index created under the same name as transformer, the change vector
                     which represents history of the object will be preserved
                    */
                    Assert.Equal(3, metadata.ChangeVector[0].Etag);
                }
            }
        }

        [Fact]
        public async Task Manually_removed_indexes_would_remove_metadata_on_startup()
        {
            var pathPrefix = Guid.NewGuid().ToString();
            var databasePath = string.Empty;
            var indexesPath = string.Empty;

            try
            {
                var userByAgeIndex = new UserByAgeIndex();
                var userByNameIndex = new UserByNameIndex();
                var usernameToUpperTransformer = new UsernameToUpperTransformer();


                DocumentDatabase databaseStore;
                using (var store = GetDocumentStore(path: pathPrefix))
                {
                    userByNameIndex.Execute(store);
                    userByAgeIndex.Execute(store);
                    usernameToUpperTransformer.Execute(store);
                    databaseStore = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                    TransactionOperationContext context;
                    using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        Assert.NotNull(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByNameIndex.IndexName));
                        Assert.NotNull(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAgeIndex.IndexName));
                        Assert.NotNull(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, usernameToUpperTransformer.TransformerName));
                    }
                }

                indexesPath = databaseStore.Configuration.Indexing.StoragePath.FullPath;
                databasePath = databaseStore.Configuration.Core.DataDirectory.FullPath;
                foreach (var indexFolder in Directory.GetDirectories(indexesPath))
                    IOExtensions.DeleteDirectory(indexFolder);

                using (var store = GetDocumentStore(path: pathPrefix))
                {
                    databaseStore = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);

                    TransactionOperationContext context;
                    using (databaseStore.ConfigurationStorage.ContextPool.AllocateOperationContext(out context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        Assert.Null(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByNameIndex.IndexName));
                        Assert.Null(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, userByAgeIndex.IndexName));
                        Assert.Null(
                            databaseStore.IndexMetadataPersistence.GetIndexMetadataByName(tx.InnerTransaction, context, usernameToUpperTransformer.TransformerName));
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(databasePath);
                IOExtensions.DeleteDirectory(indexesPath);
            }
        }
    }
}



