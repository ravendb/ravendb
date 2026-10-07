using System.Collections.Generic;
using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Queries;

public static class InventoryQueries
{
    public static List<InventoryItem> GetByWarehouse(IDocumentSession session, string warehouseId)
        => session.Query<InventoryItem>()
                  .Where(i => i.WarehouseId == warehouseId)
                  .OrderBy(i => i.ProductId)
                  .Take(200)
                  .ToList();

    public static List<InventoryItem> GetLowStock(IDocumentSession session)
        => session.Query<InventoryItem>()
                  .Where(i => i.QuantityOnHand < i.ReorderLevel)
                  .Take(100)
                  .ToList();

    public static List<Warehouse> GetActiveWarehouses(IDocumentSession session)
        => session.Query<Warehouse>()
                  .Where(w => w.Active)
                  .OrderBy(w => w.Name)
                  .Take(50)
                  .ToList();

    public static List<Supplier> GetSuppliers(IDocumentSession session)
        => session.Query<Supplier>()
                  .OrderBy(s => s.Name)
                  .Take(50)
                  .ToList();
}
