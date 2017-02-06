// -----------------------------------------------------------------------
//  <copyright file="RDoc_391.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RDoc_391 : RavenNewTestBase
    {
        private class People_By_Name_Different : AbstractIndexCreationTask<Person>
        {
            public override string IndexName
            {
                get
                {
                    return "People/By/Name";
                }
            }

            public People_By_Name_Different()
            {
                Map = persons => from person in persons select new { person.Name, Count = 1 };
            }
        }

        private class People_By_Name : AbstractIndexCreationTask<Person>
        {
            public People_By_Name()
            {
                Map = persons => from person in persons select new { person.Name };
            }
        }

        private class People_By_Name_With_Scripts : AbstractScriptedIndexCreationTask<Person>
        {
            public override string IndexName
            {
                get
                {
                    return "People/By/Name";
                }
            }

            public People_By_Name_With_Scripts()
            {
                Map = persons => from person in persons select new { person.Name };

                IndexScript = @"index";

                DeleteScript = @"delete";

                RetryOnConcurrencyExceptions = false;
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void GetIndexStatistics_should_not_advance_last_indexed_etag()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                var index = new People_By_Name_With_Scripts();
                index.Execute(store);
                WaitForIndexing(store);
                var statsBefore = store.Admin.Send(new GetStatisticsOperation());
                var indexStats = statsBefore.Indexes.First(x => x.Name == index.IndexName);
                //var lastIndexedEtag = indexStats.LastIndexedEtag;

                var statsAfter = store.Admin.Send(new GetStatisticsOperation());
                indexStats = statsAfter.Indexes.First(x => x.Name == index.IndexName);
                //Assert.Equal(lastIndexedEtag, indexStats.LastIndexedEtag);
            }
        }


        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocument1()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                //IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(People_By_Name_With_Scripts))), store);

                var index = new People_By_Name_With_Scripts();
                var indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);

                using (var session = store.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocument2()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                var index = new People_By_Name_With_Scripts();
                index.Execute(store);

                var indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);

                using (var session = store.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocumentOnShardedStore1()
        {
            using (var store1 = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            using (var store2 = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            //using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
            //                                                              {
            //                                                                  { "Shard1", store1 },
            //                                                                  { "Shard2", store2 },
            //                                                              })))
            {
                //IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(People_By_Name_With_Scripts))), store);

                var index = new People_By_Name_With_Scripts();
                var indexDefinition = store1.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);
                indexDefinition = store2.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);

                using (var session = store1.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }

                using (var session = store2.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocumentOnShardedStore2()
        {
            using (var store1 = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            using (var store2 = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            //using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
            //                                                              {
            //                                                                  { "Shard1", store1 },
            //                                                                  { "Shard2", store2 },
            //                                                              })))
            {
                var index = new People_By_Name_With_Scripts();
                //index.Execute(store);

                var indexDefinition = store1.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);
                indexDefinition = store2.Admin.Send(new GetIndexOperation(index.IndexName));
                Assert.NotNull(indexDefinition);

                using (var session = store1.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }

                using (var session = store2.OpenSession())
                {
                    var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
                    Assert.NotNull(indexDocument);
                    Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
                    Assert.Equal(index.IndexScript, indexDocument.IndexScript);
                    Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
                    Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
                }
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillResetIndexIfDocumentIsMissing()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }

                new People_By_Name().Execute(store);
                var index = new People_By_Name_With_Scripts();

                WaitForIndexing(store);

                var stats = store.Admin.Send(new GetStatisticsOperation());
                var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //Assert.True(EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, Etag.Empty));

                store.Admin.Send(new StopIndexingOperation());

                index.Execute(store);

                stats = store.Admin.Send(new GetStatisticsOperation());
                indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //Assert.True(indexStats.LastIndexedEtag.Equals(Etag.Empty));
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillResetIndexIfDocumentHasChanged()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }

                new People_By_Name().Execute(store);
                var index = new People_By_Name_With_Scripts();

                using (var commands = store.Commands())
                {
                    commands.Put(ScriptedIndexResults.IdPrefix + index.IndexName, null, new { }, null);
                }

                WaitForIndexing(store);

                var stats = store.Admin.Send(new GetStatisticsOperation());
                var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //Assert.True(EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, Etag.Empty));

                store.Admin.Send(new StopIndexingOperation());

                index.Execute(store);

                stats = store.Admin.Send(new GetStatisticsOperation());
                indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //Assert.True(indexStats.LastIndexedEtag.Equals(Etag.Empty));
            }
        }

        [Fact(Skip = "Missing feature: ScriptedIndexResults")]
        public void AbstractScriptedIndexCreationTaskWillNotResetIndexIfNothingHasChanged()
        {
            using (var store = GetDocumentStore(modifyDatabaseDocument: document => document.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults"))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Name1"
                    });

                    session.SaveChanges();
                }


                var index = new People_By_Name_With_Scripts();
                index.Execute(store);

                WaitForIndexing(store);

                var stats = store.Admin.Send(new GetStatisticsOperation());
                var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //var lastIndexedEtag = indexStats.LastIndexedEtag;
                //Assert.True(EtagUtil.IsGreaterThan(lastIndexedEtag, Etag.Empty));

                store.Admin.Send(new StopIndexingOperation());

                index.Execute(store);

                stats = store.Admin.Send(new GetStatisticsOperation());
                indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                //Assert.True(indexStats.LastIndexedEtag.Equals(lastIndexedEtag) ||
                //    EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, lastIndexedEtag));
            }
        }
    }
}
