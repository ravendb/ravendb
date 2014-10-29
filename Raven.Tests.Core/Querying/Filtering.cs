// -----------------------------------------------------------------------
//  <copyright file="Includes.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Core.Querying
{
    public class Filtering : RavenCoreTestBase
    {
		[Fact]
        public void BasicFiltering()
        {
            using (var store = GetDocumentStore())
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
                    Assert.Equal(3, companies[0].Contacts.Count);
                    Assert.Equal(1, companies[1].Contacts.Count);

                    companies = session.Query<Company>()
                        .Where(c => c.Name.In(new[] { "Company1", "Company3" }))
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    Assert.Equal("Company1", companies[0].Name);
                    Assert.Equal("Company3", companies[1].Name);
                }
            }
        }
    }
}
