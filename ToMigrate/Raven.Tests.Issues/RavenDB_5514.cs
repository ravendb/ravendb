using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_5514 : RavenTest
    {
        [Fact]
        public void ShouldReportCorrectErrorWhenUsingTooManyBooleanClausesIsThrown()
        {

            var l = new List<String>();
            for (var i = 0; i < 1040; i++)
            {
                l.Add("orders/" + i);
            }
            using ( var store = NewDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < 1040; i++)
                    {
                        var id = "orders/" + i;
                        l.Add(id);
                        bulk.Store(new Order(),id);
                    }                    
                }
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    Assert.Throws(typeof(System.InvalidOperationException),() => session.Query<Order>().Where(x => x.Id.In(l)).ToList());
                }
            }

        }
        public class Order
        {
            public string Id { get; set; }

        }
    }
}
