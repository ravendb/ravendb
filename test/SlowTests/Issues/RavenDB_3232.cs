// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3232.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3232 : RavenTestBase
    {
        private class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        private class TestIndex : AbstractIndexCreationTask<Person>
        {
            public TestIndex()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName };
            }
        }

        [Fact]
        public void ShouldSimplyCreateIndex()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                // since index dones't exists just it should simply create it instead of using side-by-side.
                new TestIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, TestIndex>()
                        .Count(x => x.LastName == "Doe");
                }
            }
        }

        [Fact]
        public void ReplaceOfNonStaleIndex()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var oldIndexDef = new IndexDefinition
                {
                    Name = "TestIndex",
                    Maps = { "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}" }
                };

                store.Admin.Send(new PutIndexesOperation(oldIndexDef));

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                store.Admin.Send(new StopIndexingOperation());

                new TestIndex().Execute(store);

                var e = Assert.Throws<RavenException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, TestIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query/sort on fields that are not indexed", e.InnerException.Message);

                store.Admin.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, TestIndex>()
                        .Count(x => x.LastName == "Doe");

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public async Task ReplaceOfNonStaleIndexAsync()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var oldIndexDef = new IndexDefinition
                {
                    Name = "TestIndex",
                    Maps = { "from person in docs.People\nselect new {\n\tFirstName = person.FirstName\n}" }
                };

                await store.Admin.SendAsync(new PutIndexesOperation(oldIndexDef));

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                await store.Admin.SendAsync(new StopIndexingOperation());

                await new TestIndex().ExecuteAsync(store);

                var e = Assert.Throws<RavenException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, TestIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query/sort on fields that are not indexed", e.InnerException.Message);

                await store.Admin.SendAsync(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, TestIndex>()
                        .Count(x => x.LastName == "Doe");

                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void SideBySideExecuteShouldNotCreateReplacementIndexIfIndexToReplaceIsIdentical()
        {
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path
            }))
            {
                var indexName = new TestIndex().IndexName;

                new TestIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                store.Admin.Send(new StopIndexingOperation());

                new TestIndex().Execute(store);

                Assert.Null(store.Admin.Send(new GetIndexOperation(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexName)));
            }
        }
    }
}
