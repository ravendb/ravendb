using System.Collections.Generic;

namespace Raven.Analyzers.Playground.Models;

public class Order
{
    public string Id { get; set; } = default!;
    public string CustomerId { get; set; } = default!;
    public string Status { get; set; } = default!;
    public decimal Total { get; set; }
    public string Region { get; set; } = default!;
    public List<OrderLine> Lines { get; set; } = [];
}

public class OrderLine
{
    public string ProductId { get; set; } = default!;
    public string Product { get; set; } = default!;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal Discount { get; set; }
}

public class Product
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string CategoryId { get; set; } = default!;
    public string SupplierId { get; set; } = default!;
    public decimal UnitPrice { get; set; }
    public int UnitsInStock { get; set; }
    public bool Discontinued { get; set; }
}

public class Customer
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string Email { get; set; } = default!;
    public string Phone { get; set; } = default!;
    public Address Address { get; set; } = default!;
    public string Region { get; set; } = default!;
}
