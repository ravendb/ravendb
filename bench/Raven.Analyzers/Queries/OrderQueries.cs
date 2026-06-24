using System.Collections.Generic;
using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Queries;

public static class OrderQueries
{
    public static List<Order> GetByStatus(IDocumentSession session, string status)
        => session.Query<Order>()
                  .Where(o => o.Status == status)
                  .Take(50)
                  .ToList();

    public static List<Order> GetByCustomer(IDocumentSession session, string customerId)
        => session.Query<Order>()
                  .Where(o => o.CustomerId == customerId)
                  .OrderBy(o => o.Total)
                  .Take(20)
                  .ToList();

    public static List<OrderSummary> GetSummaries(IDocumentSession session, string region)
        => session.Query<Order>()
                  .Where(o => o.Region == region)
                  .OrderByDescending(o => o.Total)
                  .Take(25)
                  .Select(o => new OrderSummary { Id = o.Id, Total = o.Total })
                  .ToList();

    public static Order? GetSingle(IDocumentSession session, string id)
        => session.Query<Order>()
                  .Where(o => o.Id == id)
                  .Take(1)
                  .FirstOrDefault();
}

public class OrderSummary
{
    public string Id { get; set; } = default!;
    public decimal Total { get; set; }
}
