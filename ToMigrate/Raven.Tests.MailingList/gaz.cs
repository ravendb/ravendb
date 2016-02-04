// -----------------------------------------------------------------------
//  <copyright file="gaz.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class gaz : RavenTest
    {
        public class Foo
        {
            public string Id { get; set; }
            public int CatId { get; set; }
            public double Lat { get; set; }
            public double Long { get; set; }
        }

        public class Foos : AbstractIndexCreationTask<Foo>
        {
            public Foos()
            {
                Map = foos => from foo in foos
                              select new { foo.Id, foo.CatId, _ = SpatialGenerate("Position", foo.Lat, foo.Long) };
            }
        }

        [Fact]
        public void SpatialSearchBug2()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var foo = new Foo() { Lat = 20, Long = 20, CatId = 1 };
                    session.Store(foo);
                    session.SaveChanges();

                    new Foos().Execute(store);

                    WaitForIndexing(store);

                    var query2 = session.Advanced.LuceneQuery<Foo, Foos>()
                        .UsingDefaultOperator(QueryOperator.And)
                        .WithinRadiusOf("Position", 100, 20, 20, SpatialUnits.Miles)
                        .Where("CatId:2")
                        .Where("CatId:1")
                        .ToList();

                    Assert.Empty(query2);
                }
            }
        }
    }
}
