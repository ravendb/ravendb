//-----------------------------------------------------------------------
// <copyright file="CanProjectIdFromDocumentInQueries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanProjectIdFromDocumentInQueries : RavenTestBase
    {
        public CanProjectIdFromDocumentInQueries(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void SelectIdFromDocumentWithIndexedQuery(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var indexDefinition = new IndexDefinitionBuilder<Shipment>()
                {
                    Map = docs => from doc in docs
                        select new
                        {
                            doc.Id
                        }
                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "AmazingIndex";
                store.Maintenance.Send(new PutIndexesOperation(new[] {indexDefinition}));
              

                using (var session = store.OpenSession())
                {
                    session.Store(new Shipment()
                    {
                        Id = "shipment1",
                        Name = "Some shipment"
                    });
                    session.SaveChanges();

                    var shipment = session.Query<Shipment>("AmazingIndex")
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Select(x => new Shipment
                        {
                            Id = x.Id,
                            Name = x.Name
                        }).Take(1).SingleOrDefault();

                    Assert.NotNull(shipment.Id);
                }
            }
        }

        [Fact]
        public void SelectIdFromDocumentWithDynamicQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Shipment()
                    {
                        Id = "shipment1",
                        Name = "Some shipment"
                    });
                    session.SaveChanges();

                    var shipment = session.Query<Shipment>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new Shipment()
                        {
                            Id = x.Id,
                            Name = x.Name
                        }).SingleOrDefault();

                    Assert.NotNull(shipment.Id);
                }
            }
        }

        private class Shipment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
