using System.Collections.Generic;
using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Queries;

public static class FinanceQueries
{
    public static List<Invoice> GetOverdueInvoices(IDocumentSession session, string status)
        => session.Query<Invoice>()
                  .Where(i => i.Status == status)
                  .OrderBy(i => i.DueAt)
                  .Take(50)
                  .ToList();

    public static List<Invoice> GetByCustomer(IDocumentSession session, string customerId)
        => session.Query<Invoice>()
                  .Where(i => i.CustomerId == customerId)
                  .OrderByDescending(i => i.IssuedAt)
                  .Take(20)
                  .ToList();

    public static List<Payment> GetPaymentsByMethod(IDocumentSession session, string method)
        => session.Query<Payment>()
                  .Where(p => p.Method == method)
                  .OrderByDescending(p => p.PaidAt)
                  .Take(100)
                  .ToList();

    public static List<Quote> GetOpenQuotes(IDocumentSession session, string accountId)
        => session.Query<Quote>()
                  .Where(q => q.AccountId == accountId && q.Status == "Open")
                  .OrderBy(q => q.ExpiresAt)
                  .Take(25)
                  .ToList();
}
