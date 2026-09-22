using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Indexes;

public class Inventory_ByWarehouse : AbstractIndexCreationTask<InventoryItem>
{
    public Inventory_ByWarehouse()
    {
        Map = items => from i in items
                       select new
                       {
                           i.WarehouseId,
                           i.ProductId,
                           i.QuantityOnHand
                       };
    }
}

public class Inventory_BySupplier : AbstractIndexCreationTask<InventoryItem, Inventory_BySupplier.Result>
{
    public class Result
    {
        public string ProductId { get; set; } = default!;
        public int TotalStock { get; set; }
    }

    public Inventory_BySupplier()
    {
        Map = items => from i in items
                       select new Result
                       {
                           ProductId = i.ProductId,
                           TotalStock = i.QuantityOnHand
                       };

        Reduce = results => from r in results
                            group r by r.ProductId into g
                            select new Result
                            {
                                ProductId = g.Key,
                                TotalStock = g.Sum(x => x.TotalStock)
                            };
    }
}
