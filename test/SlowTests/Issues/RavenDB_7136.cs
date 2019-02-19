using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7136 : RavenTestBase
    {
        private class InvalidMultiMap : AbstractMultiMapIndexCreationTask<Order>
        {
            public InvalidMultiMap()
            {
                AddMap<Order>(orders => from o in orders
                                        select new
                                        {
                                            Company = o.Company,
                                            Freight = o.Freight
                                        });

                AddMap<Order>(orders => from o in orders
                                        from l in o.Lines
                                        select new
                                        {
                                            Company = o.Company,
                                            Freight = o.Freight / (o.Company.Length - o.Company.Length)
                                        });

                AddMap<Order>(orders => from o in orders
                                        select new
                                        {
                                            Company = o.Employee,
                                            Freight = o.Freight
                                        });
            }
        }

        [Fact]
        public void IfOneOfTheMultiMapFunctionsIsFailingWeNeedToResetTheEnumeratorToAvoidApplyingWrongFunctionOnPreviousDocument()
        {
            using (var store = GetDocumentStore())
            {
                new InvalidMultiMap().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "companies/1",
                        Freight = 10,
                        Lines = new List<OrderLine>
                        {
                            new OrderLine(),
                            new OrderLine(),
                            new OrderLine()
                        }
                    });

                    session.Store(new Order { Company = "companies/2", Freight = 15 });

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                var indexes = WaitForIndexingErrors(store);
                var error = indexes.Single();
                Assert.Equal(1, error.Errors.Length);
                Assert.Contains(nameof(DivideByZeroException), error.Errors[0].Error);
            }
        }
    }
}
