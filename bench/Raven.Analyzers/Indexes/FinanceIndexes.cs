using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Indexes;

public class Invoices_ByDate : AbstractIndexCreationTask<Invoice>
{
    public Invoices_ByDate()
    {
        Map = invoices => from i in invoices
                          select new
                          {
                              i.CustomerId,
                              i.IssuedAt,
                              i.DueAt,
                              i.Amount,
                              i.Status
                          };
    }
}

public class Quotes_ByAccount : AbstractIndexCreationTask<Quote>
{
    public Quotes_ByAccount()
    {
        Map = quotes => from q in quotes
                        select new
                        {
                            q.AccountId,
                            q.CustomerId,
                            q.Status,
                            q.Total,
                            q.ExpiresAt
                        };
    }
}
