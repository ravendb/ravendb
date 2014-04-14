using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
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
                IndexCreation.CreateIndexes(typeof(Companies_CompanyByType).Assembly, store);

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

                    Companies_CompanyByType.ReduceResult[] companies = session.Query<Companies_CompanyByType.ReduceResult>("Companies/CompanyByType")
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

        private class Companies_CompanyByType : AbstractIndexCreationTask<Company, Companies_CompanyByType.ReduceResult>
        {
            public class ReduceResult
            {
                public Company.CompanyType Type { get; set; }
                public int ContactsCount { get; set; }
                public DateTime LastModified { get; set; }
            }

            public Companies_CompanyByType()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       Type = company.Type,
                                       ContactsCount = company.Contacts.Count,
                                       LastModified = MetadataFor(company).Value<DateTime>("Last-Modified")
                                   };

                Reduce = results => from result in results
                                    group result by result.Type
                                        into g
                                        select new
                                        {
                                            Type = g.Key,
                                            ContactsCount = g.Sum(x => x.ContactsCount),
                                            LastModified = g.Select(x => x.LastModified).First()
                                        };
            }
        }
    }
}
