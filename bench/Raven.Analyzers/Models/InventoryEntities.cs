namespace Raven.Analyzers.Playground.Models;

public class InventoryItem
{
    public string Id { get; set; } = default!;
    public string ProductId { get; set; } = default!;
    public string WarehouseId { get; set; } = default!;
    public int QuantityOnHand { get; set; }
    public int ReorderLevel { get; set; }
    public decimal UnitCost { get; set; }
}

public class Warehouse
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public Address Location { get; set; } = default!;
    public int Capacity { get; set; }
    public bool Active { get; set; }
}

public class Supplier
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string ContactName { get; set; } = default!;
    public string Email { get; set; } = default!;
    public Address Address { get; set; } = default!;
}

public class Category
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Description { get; set; } = default!;
    public string ParentCategoryId { get; set; } = default!;
}
