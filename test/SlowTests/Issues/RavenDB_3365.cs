// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3365.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3365 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-4185")]
        public void index_pretty_printer_ignores_whitespaces()
        {
            var firstFormat = IndexPrettyPrinter.TryFormat("from order in docs.Orders select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }");
            var secondFormat = IndexPrettyPrinter.TryFormat("from order  \t   in docs.Orders       select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }");

            Assert.Equal(firstFormat, secondFormat);
        }

        [Fact]
        public void shouldnt_reset_index_when_non_meaningful_change()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                // now fetch index definition modify map (only by giving extra write space)
                var indexName = new Users_ByName().IndexName;
                var indexDef = store.Maintenance.Send(new GetIndexOperation(indexName));

                var indexInstance1 = GetDocumentDatabaseInstanceFor(store).Result.IndexStore.GetIndex(indexName);

                
                indexDef.Maps = new HashSet<string> { "   " + indexDef.Maps.First().Replace(" ", "  \t ") + "   " };
                store.Maintenance.Send(new PutIndexesOperation(indexDef));

                var indexInstance2 = GetDocumentDatabaseInstanceFor(store).Result.IndexStore.GetIndex(indexName);
                Assert.Same(indexInstance1, indexInstance2);
            }
        }

        private static void Setup(IDocumentStore store)
        {
            new Users_ByName().Execute(store);
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 20; i++)
                {
                    session.Store(new User());
                }
                session.SaveChanges();
            }

            WaitForIndexing(store);
            store.Maintenance.Send(new StopIndexingOperation());
        }
    }
}
