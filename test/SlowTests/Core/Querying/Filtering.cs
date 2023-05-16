// -----------------------------------------------------------------------
//  <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using Contact = SlowTests.Core.Utils.Entities.Contact;

namespace SlowTests.Core.Querying
{
    public class Filtering : RavenTestBase
    {
        public Filtering(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void BasicFiltering(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var contact1 = new Contact { FirstName = "First Expression Name" };
                    var contact2 = new Contact { FirstName = "Expression First Name" };
                    var contact3 = new Contact { FirstName = "First Name" };
                    session.Store(contact1);
                    session.Store(contact2);
                    session.Store(contact3);
                    session.Store(new Company { Name = "Company1", Contacts = new List<Contact> { contact1, contact2, contact3 } });
                    session.Store(new Company { Name = "Company2", Contacts = new List<Contact> { contact1, contact2 } });
                    session.Store(new Company { Name = "Company3", Contacts = new List<Contact> { contact3 } });
                    session.SaveChanges();

                    var companies = session.Query<Company>()
                        .Where(x => x.Contacts.Any(contact => contact.FirstName == "First Name"))
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    Assert.Contains(3, companies.Select(x => x.Contacts.Count));
                    Assert.Contains(1, companies.Select(x => x.Contacts.Count));

                    companies = session.Query<Company>()
                        .Where(c => c.Name.In(new[] { "Company1", "Company3" }))
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    Assert.Contains("Company1", companies.Select(x => x.Name));
                    Assert.Contains("Company3", companies.Select(x => x.Name));
                }
            }
        }
    }
}
