// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3232.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4025 : RavenTest
    {
        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
        }

        public class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        public class TestIndex : AbstractIndexCreationTask<Person>
        {
            public TestIndex()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName };
            }
        }

        [Fact]
        public void CanRenameSimpleIndex()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                var indexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}"
                };
                store.DatabaseCommands.PutIndex("TestIndex", indexDef);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var testIndex = store.SystemDatabase.Indexes.GetIndexDefinition("TestIndex");

                store.SystemDatabase.Indexes.RenameIndex(testIndex, "RenamedTestIndex");

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person>("RenamedTestIndex")
                        .Count(x => x.FirstName == "John");

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void CannotRenameLockedIndex()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                var indexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}",
                    LockMode = IndexLockMode.LockedError
                };
                store.DatabaseCommands.PutIndex("TestIndex", indexDef);

                var testIndex = store.SystemDatabase.Indexes.GetIndexDefinition("TestIndex");

                Assert.Throws<InvalidOperationException>(() => store.SystemDatabase.Indexes.RenameIndex(testIndex, "RenamedTestIndex"));
            }
        }

        [Fact]
        public void CanRenameScriptedIndex()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                // arrange
                using (var s = store.OpenSession())
                {
                    s.Store(new ScriptedIndexResults
                    {
                        Id = ScriptedIndexResults.IdPrefix + new TestIndex().IndexName,
                        IndexScript = "PutDocument(this.LastName, { Name: this.LastName })"
                    });
                    s.SaveChanges();
                }

                var indexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName, LastName = person.LastName\n}"
                };
                store.DatabaseCommands.PutIndex("TestIndex", indexDef);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var testIndex = store.SystemDatabase.Indexes.GetIndexDefinition("TestIndex");

                // act
                store.SystemDatabase.Indexes.RenameIndex(testIndex, "RenamedTestIndex");

                // make sure newly inserted document will also be processed by scripted index
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "Marcin", LastName = "Lewandowski" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                // assert
                using (var session = store.OpenSession())
                {
                    var john = session
                        .Query<Person>("RenamedTestIndex")
                        .First(x => x.FirstName == "John");

                    Assert.Equal("Doe", john.LastName);

                    var marcin = session
                        .Query<Person>("RenamedTestIndex")
                        .First(x => x.FirstName == "Marcin");

                    Assert.Equal("Lewandowski", marcin.LastName);

                    Assert.NotNull(store.DatabaseCommands.Get("Doe"));
                    Assert.NotNull(store.DatabaseCommands.Get("Lewandowski"));
                }
            }
        }

        [Fact]
        public void IndexingErrorsAreRetainedAfterRename()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                // arrange 
                var indexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName, Age = 1 /  new Random().Next(0,0) \n}",
                };
                store.DatabaseCommands.PutIndex("TestIndex", indexDef);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var stats = store.DatabaseCommands.GetStatistics();
                Assert.True(stats.Errors.Any(x => x.IndexName == "TestIndex"));

                // act 
                var testIndex = store.SystemDatabase.Indexes.GetIndexDefinition("TestIndex");
                store.SystemDatabase.Indexes.RenameIndex(testIndex, "RenamedTestIndex");

                // assert
                stats = store.DatabaseCommands.GetStatistics();
                Assert.False(stats.Errors.Any(x => x.IndexName == "TestIndex"));
                Assert.True(stats.Errors.Any(x => x.IndexName == "RenamedTestIndex"));
            }
        }

        [Fact]
        public void IndexRenameRetainsLastQueryTime()
        {
            using (var store = NewDocumentStore(runInMemory: false))
            {
                // arrange 
                var indexDef = new IndexDefinition
                {
                    Map = "from person in docs.People\nselect new {\n\tFirstName = person.FirstName \n}",
                };
                store.DatabaseCommands.PutIndex("TestIndex", indexDef);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session
                        .Query<Person>("TestIndex")
                        .First(x => x.FirstName == "John");
                }

                var stats = store.DatabaseCommands.GetStatistics();
                var indexStats = stats.Indexes.First(x => x.Name == "TestIndex");
                var lastQueryTime = indexStats.LastQueryTimestamp;

                // act 
                var testIndex = store.SystemDatabase.Indexes.GetIndexDefinition("TestIndex");
                store.SystemDatabase.Indexes.RenameIndex(testIndex, "RenamedTestIndex");

                // assert
                stats = store.DatabaseCommands.GetStatistics();
                Assert.False(stats.Indexes.Any(x => x.Name == "TestIndex"));
                indexStats = stats.Indexes.First(x => x.Name == "RenamedTestIndex");
                var newIndexLastQueryTime = indexStats.LastQueryTimestamp;
                Assert.Equal(lastQueryTime, newIndexLastQueryTime);
            }
        }


     
    }
}
