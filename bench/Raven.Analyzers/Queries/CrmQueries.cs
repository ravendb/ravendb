using System.Collections.Generic;
using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Linq;

namespace Raven.Analyzers.Playground.Queries;

public static class CrmQueries
{
    public static List<Lead> GetLeadsByOwner(IDocumentSession session, string ownerId)
        => session.Query<Lead>()
                  .Where(l => l.OwnerId == ownerId)
                  .OrderByDescending(l => l.EstimatedValue)
                  .Take(50)
                  .ToList();

    public static List<Lead> GetOpenLeads(IDocumentSession session)
        => session.Query<Lead>()
                  .Where(l => l.Status == "Open")
                  .OrderBy(l => l.CreatedAt)
                  .Take(100)
                  .ToList();

    public static List<Contact> GetContactsByAccount(IDocumentSession session, string accountId)
        => session.Query<Contact>()
                  .Where(c => c.AccountId == accountId)
                  .OrderBy(c => c.LastName)
                  .Take(50)
                  .ToList();

    public static List<Company> GetCompaniesByIndustry(IDocumentSession session, string industry)
        => session.Query<Company>()
                  .Where(c => c.Industry == industry)
                  .OrderBy(c => c.Name)
                  .Take(50)
                  .ToList();
}
