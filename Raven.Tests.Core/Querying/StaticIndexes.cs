using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Querying
{
    public class StaticIndexes : RavenCoreTestBase
    {
        [Fact]
        public void CreateAndQuerySimpleMapReduceIndexWithMetadataForCall()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_CompanyByType().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var contact1 = new Contact { FirstName = "FirstName1" };
                    var contact2 = new Contact { FirstName = "FirstName2" };
                    var contact3 = new Contact { FirstName = "FirstName3" };
                    session.SaveChanges();

                    session.Store(new Company 
                        {
                            Type = Company.CompanyType.Public, 
                            Contacts = new List<Contact> {contact1, contact2, contact3}
                        });
                    session.Store(new Company 
                        {
                            Type = Company.CompanyType.Public, 
                            Contacts = new List<Contact> {contact3}
                        });
                    session.Store(new Company 
                        {
                            Type = Company.CompanyType.Public, 
                            Contacts = new List<Contact> {contact1, contact2}
                        });
                    session.Store(new Company 
                        {
                            Type = Company.CompanyType.Private, 
                            Contacts = new List<Contact> {contact1, contact2}
                        });
                    session.Store(new Company 
                        {
                            Type = Company.CompanyType.Private, 
                            Contacts = new List<Contact> {contact1, contact2, contact3}
                        });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    Companies_CompanyByType.ReduceResult[] companies = session.Query<Companies_CompanyByType.ReduceResult, Companies_CompanyByType>()
                        .OrderBy(x => x.Type)
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    Assert.Equal(Company.CompanyType.Private, companies[0].Type);
                    Assert.Equal(5, companies[0].ContactsCount);
                    Assert.NotNull(companies[0].LastModified);
                    Assert.Equal(Company.CompanyType.Public, companies[1].Type);
                    Assert.Equal(6, companies[1].ContactsCount);
                    Assert.NotNull(companies[1].LastModified);
                }
            }
        }

        [Fact]
        public void CreateAndQueryIndexContainingAllDocumentFields()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_AllProperties().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Name2", Address1 = "Address1" });
                    session.Store(new Company { Name = "Name0", Address1 = "Some Address" });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var companies = session.Query<Companies_AllProperties.Result, Companies_AllProperties>()
                        .Where(x => x.Query == "Address1")
                        .OfType<Company>()
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Address1", companies[0].Address1);
                }
            }
        }

        [Fact]
        public void CreateAndQuerySimpleIndexWithSortingAndCustomCollateral()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_SortByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "C" });
                    session.Store(new Company { Name = "a" });
                    session.Store(new Company { Name = "ć" });
                    session.Store(new Company { Name = "ą" });
                    session.Store(new Company { Name = "A" });
                    session.Store(new Company { Name = "c" });
                    session.Store(new Company { Name = "Ą" });
                    session.Store(new Company { Name = "D" });
                    session.Store(new Company { Name = "d" });
                    session.Store(new Company { Name = "b" });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var companies = session.Query<Company, Companies_SortByName>()
                        .OrderBy(c => c.Name)
                        .ToArray();
                    Assert.Equal(10, companies.Length);
                    Assert.Equal("a", companies[0].Name);
                    Assert.Equal("A", companies[1].Name);
                    Assert.Equal("ą", companies[2].Name);
                    Assert.Equal("Ą", companies[3].Name);
                    Assert.Equal("b", companies[4].Name);
                    Assert.Equal("c", companies[5].Name);
                    Assert.Equal("C", companies[6].Name);
                    Assert.Equal("ć", companies[7].Name);
                    Assert.Equal("d", companies[8].Name);
                    Assert.Equal("D", companies[9].Name);
                }
            }
        }
    }
}
