using System.Linq;
using Raven.Analyzers.Playground.Models;
using Raven.Client.Documents.Indexes;

namespace Raven.Analyzers.Playground.Indexes;

public class Leads_ByOwner : AbstractIndexCreationTask<Lead>
{
    public Leads_ByOwner()
    {
        Map = leads => from l in leads
                       select new
                       {
                           l.OwnerId,
                           l.Status,
                           l.Name,
                           l.EstimatedValue,
                           l.CreatedAt
                       };
    }
}

public class Contacts_ByCompany : AbstractIndexCreationTask<Contact>
{
    public Contacts_ByCompany()
    {
        Map = contacts => from c in contacts
                          select new
                          {
                              c.AccountId,
                              c.FirstName,
                              c.LastName,
                              c.Email
                          };

        Index(x => x.FirstName, FieldIndexing.Search);
        Index(x => x.LastName, FieldIndexing.Search);
    }
}
