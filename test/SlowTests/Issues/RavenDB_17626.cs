using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17626 : RavenTestBase
{
    public RavenDB_17626(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void EnumerableInSelectManyWillBeCastedProperly()
    {
        using (var store = GetDocumentStore())
        {
            store.ExecuteIndex(new Index());
            var date = DateTime.UtcNow;
            using (var session = store.OpenSession())
            {
                session.Store(new Product { Dates = new List<Date> { new Date { DateTime = date },new Date { DateTime = date.AddDays(1) } } });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
            using (var session = store.OpenSession())
            {
                var result = session.Query<Index.Result, Index>()
                    .Where(x => x.Dates.Any(d => d == date))
                    .ProjectInto<Index.Result>()
                    .ToList();
                Assert.Equal(1, result.Count);
                Assert.Equal(date, result[0].Dates.First());
            }
        }
    }

    private class Product
    {
        public List<Date> Dates { get; set; }
    }

    private class Date
    {
        public DateTime DateTime { get; set; }
    }
    
    private class Index : AbstractIndexCreationTask<Product, Index.Result>
    {
        public Index()
        {
            Map = products => from product in products
                let productByDate = from date in product.Dates
                    select new { Dates = new List<DateTime> { date.DateTime } }
                select new Result { Dates = productByDate.SelectMany(x => x.Dates).ToList(), };
            
            Store("Dates", FieldStorage.Yes);
        }

        public class Result
        {
            public List<DateTime> Dates
            {
                get;
                set;
            }
        }
    }
}
