// -----------------------------------------------------------------------
//  <copyright file="gaz.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.MailingList
{
    public class gaz : RavenTestBase
    {
        private class Foo
        {
            public string Id { get; set; }
            public int CatId { get; set; }
            public double Lat { get; set; }
            public double Long { get; set; }
        }

        private class Foos : AbstractIndexCreationTask<Foo>
        {
            public Foos()
            {
                Map = foos => from foo in foos
                              select new { foo.Id, foo.CatId, _ = SpatialGenerate("Position", foo.Lat, foo.Long) };
            }
        }

        [Fact(Skip = "Missing feature: Spatial")]
        public void SpatialSearchBug2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var foo = new Foo() { Lat = 20, Long = 20, CatId = 1 };
                    session.Store(foo);
                    session.SaveChanges();

                    new Foos().Execute(store);

                    WaitForIndexing(store);

                    var query2 = session.Advanced.DocumentQuery<Foo, Foos>()
                        .UsingDefaultOperator(QueryOperator.And)
                        .WithinRadiusOf("Position", 100, 20, 20, SpatialUnits.Miles)
                        .WhereLucene("CatId", "2")
                        .WhereLucene("CatId", "1")
                        .ToList();

                    Assert.Empty(query2);
                }
            }
        }
    }
}
