using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5514 : RavenTestBase
    {
        [Fact]
        public void ShouldReportCorrectErrorWhenUsingTooManyBooleanClausesIsThrown()
        {
            var l = new List<string>();
            for (var i = 0; i < 1040; i++)
            {
                l.Add("orders/" + i);
            }
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 1040; i++)
                    {
                        var id = "orders/" + i;
                        l.Add(id);
                        bulk.Store(new Order(), id);
                    }
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var e = Assert.Throws<RavenException>(() => session.Query<Order>().Where(x => x.Id.In(l)).ToList());
                    Assert.Contains("maxClauseCount is set to", e.Message);
                }
            }
        }

        private class Order
        {
            public string Id { get; set; }

        }
    }
}
