using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class LargeQuery : RavenTestBase
    {
        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanExecuteLargeQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();

                new OrderIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    var elements = new List<string>();

                    for (int i = 0; i < 280; i++)
                    {
                        elements.Add("Orders/" + Guid.NewGuid().ToString());
                    }


                    var result = session.Query<OrderIndex.IndexResult, OrderIndex>()
                        .Where(p => p.Id.In(elements))
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .FirstOrDefault();
                }
            }
        }

        private class Order
        {
            public string Id { get; set; }
        }


        private class OrderIndex : AbstractMultiMapIndexCreationTask<OrderIndex.IndexResult>
        {
            public class IndexResult
            {
                public string Id { get; set; }
            }


            public OrderIndex()
            {
                AddMap<Order>(orders => from o in orders

                                        select new
                                        {
                                            Id = o.Id,
                                        });

                Reduce = results => from result in results
                                    group result by new { result.Id }
                                        into gr
                                    select new OrderIndex.IndexResult
                                    {
                                        Id = gr.Select(x => x.Id).FirstOrDefault(x => x != null),
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
