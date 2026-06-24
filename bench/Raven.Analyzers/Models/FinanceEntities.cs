using System;

namespace Raven.Analyzers.Playground.Models;

public class Invoice
{
    public string Id { get; set; } = default!;
    public string CustomerId { get; set; } = default!;
    public string OrderId { get; set; } = default!;
    public DateTime IssuedAt { get; set; }
    public DateTime DueAt { get; set; }
    public decimal Amount { get; set; }
    public string Status { get; set; } = default!;
}

public class Payment
{
    public string Id { get; set; } = default!;
    public string InvoiceId { get; set; } = default!;
    public string CustomerId { get; set; } = default!;
    public DateTime PaidAt { get; set; }
    public decimal Amount { get; set; }
    public string Method { get; set; } = default!;
}

public class Quote
{
    public string Id { get; set; } = default!;
    public string CustomerId { get; set; } = default!;
    public string AccountId { get; set; } = default!;
    public DateTime ExpiresAt { get; set; }
    public decimal Total { get; set; }
    public string Status { get; set; } = default!;
}

public class Account
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public string OwnerId { get; set; } = default!;
    public string Type { get; set; } = default!;
    public decimal CreditLimit { get; set; }
}
