// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2867.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2867 : RavenTestBase
    {
        public RavenDB_2867(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public string Id { get; set; }

            public string FirstName { get; set; }

            public string LastName { get; set; }
        }

        private class OldIndex : AbstractIndexCreationTask<Person>
        {
            public OldIndex()
            {
                Map = persons => from person in persons select new { person.FirstName };
            }
        }

        private class NewIndex : AbstractIndexCreationTask<Person>
        {
            public override string IndexName => "OldIndex";

            public NewIndex()
            {
                Map = persons => from person in persons select new { person.FirstName, person.LastName };
            }
        }

        [Fact]
        public void ReplaceOfNonStaleIndex()
        {
            using (var store = GetDocumentStore())
            {
                new OldIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person { FirstName = "John", LastName = "Doe" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                store.Maintenance.Send(new StopIndexingOperation());
                new NewIndex().Execute(store);

                var e = Assert.Throws<RavenException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        var count = session.Query<Person, OldIndex>()
                            .Count(x => x.LastName == "Doe");
                    }
                });

                Assert.Contains("The field 'LastName' is not indexed, cannot query/sort on fields that are not indexed", e.InnerException.Message);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<Person, OldIndex>()
                        .Count(x => x.LastName == "Doe");

                    Assert.Equal(1, count);
                }
            }
        }
    }
}
