// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4446.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4446 : RavenTest
    {
        [Fact]
        public void can_disable_precomputed_batch()
        {
            using (var store = NewDocumentStore(configureStore: documentStore =>
            {
                documentStore.Configuration.MaxPrecomputedBatchSizeForNewIndex = 0;
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{CompanyId = "companies/1"});
                    session.SaveChanges();
                }

                var index = new Orders_Index();
                index.Execute(store);

                WaitForIndexing(store);
            }
        }

        public class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }

        public class Orders_Index : AbstractIndexCreationTask<Order>
        {
            public Orders_Index()
            {
                Map = orders => from order in orders
                               select new
                               {
                                   order.CompanyId
                               };
            }
        }
    }
}
