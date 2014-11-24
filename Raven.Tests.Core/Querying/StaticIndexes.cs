using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Raven.Tests.Core.Utils.Transformers;
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

        [Fact]
        public void CreateAndQuerySimpleIndexWithCustomAnalyzersAndFieldOptions()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_CustomAnalyzers().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = 100;

                    session.Store(new Company 
                    {
                        Name = "The lazy dogs, Bob@hotmail.com 123432.",
                        Desc = "The lazy dogs, Bob@hotmail.com 123432.",
                        Email = "test Bob@hotmail.com",
                        Address1 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Address2 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Address3 = "The lazy dogs, Bob@hotmail.com 123432.",
                        Phone = 111222333
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    //StandardAnalyzer
                    var companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Name == "123432")
                        .ToArray();
                    Assert.Equal(1, companies.Length);

                    //StopAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "bob")
                        .ToArray();
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Desc == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);


                    //should not be analyzed
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "test")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Email == "test Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);

                    //SimpleAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "the")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "dogs")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "the lazy dogs")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "bob")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "hotmail")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address1 == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);

                    //WhitespaceAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "lazy")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "dogs")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "dogs,")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "bob")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "hotmail")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "com")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "123432")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address2 == "123432.")
                        .ToArray();
                    Assert.Equal(1, companies.Length);


                    //KeywordAnalyzer
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "123432.")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "the")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "lazy")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "dogs")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "Bob@hotmail.com")
                        .ToArray();
                    Assert.Equal(0, companies.Length);
                    companies = session.Query<Company, Companies_CustomAnalyzers>()
                        .Where(c => c.Address3 == "The lazy dogs, Bob@hotmail.com 123432.")
                        .ToArray();
                    Assert.Equal(1, companies.Length);


                    session.Store(new Company
                    {
                        Id = "companies/2",
                        Name = "The lazy dogs, Bob@hotmail.com lazy 123432 lazy dogs."
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    FieldHighlightings highlightings;
                    companies = session.Advanced.DocumentQuery<Company>("Companies/CustomAnalyzers")
                        .Highlight("Name", 128, 1, out highlightings)
                        .Search("Name", "lazy")
                        .ToArray();
                    Assert.Equal(2, companies.Length);
                    
                    var fragments = highlightings.GetFragments(companies[0].Id);
                    Assert.Equal(
                        "The <b style=\"background:yellow\">lazy</b> dogs, Bob@hotmail.com <b style=\"background:yellow\">lazy</b> 123432 <b style=\"background:yellow\">lazy</b> dogs.", 
                        fragments.First()
                        ); fragments = highlightings.GetFragments(companies[1].Id);
                    Assert.Equal(
                        "The <b style=\"background:yellow\">lazy</b> dogs, Bob@hotmail.com 123432.",
                        fragments.First()
                        );
                }
            }
        }

        [Fact]
        public void CreateAndQuerySimpleIndexWithRecurse()
        {
            using (var store = GetDocumentStore())
            {
                new Posts_Recurse().Execute(store);

                using (var session = store.OpenSession())
                {
                    var post1 = new Post { Title = "Post1", Desc = "Post1 desc" };
                    var post2 = new Post { Title = "Post2", Desc = "Post2 desc", Comments = new Post[] {post1} };
                    var post3 = new Post { Title = "Post3", Desc = "Post3 desc", Comments = new Post[] {post2} };
                    var post4 = new Post { Title = "Post4", Desc = "Post4 desc", Comments = new Post[] {post3} };
                    session.Store(post4);
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var posts = session.Query<Post, Posts_Recurse>()
                        .ToArray();
                    Assert.Equal(1, posts.Length);
                    Assert.Equal("Post4", posts[0].Title);
                    Assert.Equal("Post3", posts[0].Comments[0].Title);
                    Assert.Equal("Post2", posts[0].Comments[0].Comments[0].Title);
                    Assert.Equal("Post1", posts[0].Comments[0].Comments[0].Comments[0].Title);
                }
            }
        }

        [Fact]
        public void CreateAndQuerySimpleIndexWithReferencedDocuments()
        {
            using (var store = GetDocumentStore())
            {
                new Companies_WithReferencedEmployees().Execute(store);
                new CompanyEmployeesTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { Id = "employees/1", LastName = "Last Name 1" });
                    session.Store(new Employee { Id = "employees/2", LastName = "Last Name 2" });
                    session.Store(new Employee { Id = "employees/3", LastName = "Last Name 3" });
                    session.Store(new Company { Name = "Company", EmployeesIds = new List<string> { "employees/1", "employees/2", "employees/3" } });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var companies = session.Query<Company, Companies_WithReferencedEmployees>()
                        .TransformWith<CompanyEmployeesTransformer, Companies_WithReferencedEmployees.CompanyEmployees>()
                        .ToArray();
                    Assert.Equal(1, companies.Length);
                    Assert.Equal("Company", companies[0].Name);
                    Assert.NotNull(companies[0].Employees);
                    Assert.Equal("Last Name 1", companies[0].Employees[0]);
                    Assert.Equal("Last Name 2", companies[0].Employees[1]);
                    Assert.Equal("Last Name 3", companies[0].Employees[2]);
                }
            }
        }
    }
}
