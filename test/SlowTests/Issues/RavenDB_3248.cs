// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3248 : RavenTestBase
    {
        public RavenDB_3248(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void StreamQueryShouldWorkEvenIfWaitForNoneStaleResualtIsSet()
        {
            using (var store = GetDocumentStore())
            {
                new RavenDB_3248_TestObject_ByName().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 20; i++)
                    {
                        session.Store(new RavenDB_3248_TestObject());
                    }
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                int count = 0;
                using (var session = store.OpenSession())
                {
                    var query = session.Query<RavenDB_3248_TestObject, RavenDB_3248_TestObject_ByName>();

                    var reader = session.Advanced.Stream(query);

                    while (reader.MoveNext())
                    {
                        count++;
                        Assert.IsType<RavenDB_3248_TestObject>(reader.Current.Document);
                    }
                }
                Assert.Equal(count, 20);
            }

        }

        private class RavenDB_3248_TestObject_ByName : AbstractIndexCreationTask<RavenDB_3248_TestObject>
        {
            public RavenDB_3248_TestObject_ByName()
            {
                Map = users => from u in users select new { u.Name };
            }
        }

        private class RavenDB_3248_TestObject
        {
            public string Name { get; set; }
        }
    }
}

