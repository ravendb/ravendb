// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3365.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes;
using SlowTests.Core.Utils.Entities;
using SlowTests.Core.Utils.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3365 : RavenTestBase
    {
        public RavenDB_3365(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task shouldnt_reset_index_when_non_meaningful_change()
        {
            using (var store = GetDocumentStore())
            {
                Setup(store);

                // now fetch index definition modify map (only by giving extra write space)
                var indexName = new Users_ByName().IndexName;
                var indexDef = store.Maintenance.Send(new GetIndexOperation(indexName));

                var indexInstance1 = (await Databases.GetDocumentDatabaseInstanceFor(store)).IndexStore.GetIndex(indexName);

                
                indexDef.Maps = new HashSet<string> { "   " + indexDef.Maps.First().Replace(" ", "  \t ") + "   " };
                store.Maintenance.Send(new PutIndexesOperation(indexDef));

                var indexInstance2 = (await Databases.GetDocumentDatabaseInstanceFor(store)).IndexStore.GetIndex(indexName);
                Assert.Same(indexInstance1, indexInstance2);
            }
        }

        private void Setup(IDocumentStore store)
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

            Indexes.WaitForIndexing(store);
            store.Maintenance.Send(new StopIndexingOperation());
        }
    }
}
