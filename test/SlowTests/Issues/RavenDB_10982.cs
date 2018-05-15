using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10982 : RavenTestBase
    {
        [Fact]
        public void Should_remove_result_from_index_on_reduce_error()
        {
            using (var store = GetDocumentStore())
            {
                var index = new MapReduceIndexFailingOn2ndReduce();
                store.ExecuteIndex(index);

                using (var session = store.OpenSession())
                {
                    var order = new Order()
                    {
                        Lines = new List<OrderLine>()
                        {
                            new OrderLine
                            {
                                Quantity = 10
                            }
                        }
                    };
                    session.Store(order, "orders/1");

                    session.SaveChanges();

                    var results = session.Query<MapReduceIndexFailingOn2ndReduce.Result, MapReduceIndexFailingOn2ndReduce>().Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, results[0].Value);

                    order.Lines[0].Quantity = 0; // to force division by 0

                    session.SaveChanges();

                    results = session.Query<MapReduceIndexFailingOn2ndReduce.Result, MapReduceIndexFailingOn2ndReduce>().Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(0, results.Count);

                    IndexErrors[] errors = null;

                    for (int i = 0; i < 100; i++)
                    {
                        errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { index.IndexName }));

                        if (errors.Length > 0 && errors[0].Errors.Length > 0)
                            break;

                        Thread.Sleep(50);
                    }
                    
                    Assert.Equal(1, errors[0].Errors.Length);
                }
            }
        }

        private class MapReduceIndexFailingOn2ndReduce : AbstractIndexCreationTask<Order, MapReduceIndexFailingOn2ndReduce.Result>
        {
            public class Result
            {
                public int Value { get; set; }
            }

            public MapReduceIndexFailingOn2ndReduce()
            {
                Map = orders => from o in orders
                    select new
                    {
                        Value = o.Lines.Sum(x => x.Quantity)
                    };

                Reduce = results => from r in results
                    group r by 1
                    into g
                    select new
                    {
                        Value = 10 / g.Sum(x => x.Value)
                    };
            }
        }
    }
}
