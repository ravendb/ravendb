using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Indexes;

public class Orders_ByCustomer : AbstractIndexCreationTask<Order>
{
    public Orders_ByCustomer()
    {
        Map = orders => from o in orders
                        select new
                        {
                            o.CustomerId,
                            o.Status,
                            o.Total,
                            o.Region
                        };
    }
}

public class Orders_ByProduct : AbstractIndexCreationTask<Order, Orders_ByProduct.Result>
{
    public class Result
    {
        public string CustomerId { get; set; } = default!;
        public string Status { get; set; } = default!;
        public decimal Total { get; set; }
        public int LineCount { get; set; }
    }

    public Orders_ByProduct()
    {
        Map = orders => from o in orders
                        select new Result
                        {
                            CustomerId = o.CustomerId,
                            Status = o.Status,
                            Total = o.Total,
                            LineCount = o.Lines.Count
                        };

        Reduce = results => from r in results
                            group r by r.CustomerId into g
                            select new Result
                            {
                                CustomerId = g.Key,
                                Status = g.First().Status,
                                Total = g.Sum(x => x.Total),
                                LineCount = g.Sum(x => x.LineCount)
                            };
    }
}
