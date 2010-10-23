using Newtonsoft.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class ProjectionFromDynamicQuery : LocalClientTest
    {
        [Fact]
        public void ProjectNameFromDynamicQueryUsingLucene()
        {
            using(var documentStore = NewDocumentStore())
            {
                using(var s = documentStore.OpenSession())
                {
                    s.Store(new User{Name = "Ayende", Email = "Ayende@ayende.com"});
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var result = s.Advanced.DynamicLuceneQuery<User>()
                        .WhereEquals("Name", "Ayende", isAnalyzed: true)
                        .SelectFields<JObject>("Email")
                        .First();

                    Assert.Equal("Ayende@ayende.com", result.Value<string>("Email"));
                }
            }
        }

        [Fact]
        public void ProjectNameFromDynamicQueryUsingLinq()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new User { Name = "Ayende", Email = "Ayende@ayende.com" });
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var result = from user in s.Query<User>()
                                 where user.Name == "Ayende"
                                 select new { user.Email };

                    Assert.Equal("Ayende@ayende.com", result.First().Email);
                }
            }
        }

        [Fact]
        public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedObject()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new Person()
                    {
                        Name = "Ayende",
                        BillingAddress = new Address
                        {
                            City = "Bologna"
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var result = s.Advanced.DynamicLuceneQuery<Person>()
                        .WhereEquals("Name", "Ayende", isAnalyzed: true)
                        .SelectFields<JObject>("BillingAddress")
                        .First();

                    Assert.Equal("Bologna", result.Value<JObject>("BillingAddress").Value<string>("City"));
                }
            }
        }

        [Fact]
        public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedProperty()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new Person()
                    {
                        Name = "Ayende",
                        BillingAddress = new Address
                        {
                            City = "Bologna"
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var result = s.Advanced.DynamicLuceneQuery<Person>()
                        .WhereEquals("Name", "Ayende", isAnalyzed: true)
                        .SelectFields<JObject>("BillingAddress.City")
                        .First();

                    Assert.Equal("Bologna", result.Value<string>("BillingAddress.City"));
                }
            }

        }

        [Fact]
        public void ProjectNameFromDynamicQueryUsingLuceneUsingNestedArray()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var s = documentStore.OpenSession())
                {
                    s.Store(new Person()
                    {
                        Name = "Ayende",
                        BillingAddress = new Address
                        {
                            City = "Bologna"
                        },
                        Addresses = new Address[]
                        {
                            new Address {City = "Old York"},
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = documentStore.OpenSession())
                {
                    var result = s.Advanced.DynamicLuceneQuery<Person>()
                        .WhereEquals("Name", "Ayende", isAnalyzed: true)
                        .SelectFields<JObject>("Addresses[0].City")
                        .First();

                    Assert.Equal("Old York", result.Value<string>("Addresses[0].City"));
                }
            }
        }

        public class Person
        {
            public string Name { get; set; }
            public Address BillingAddress { get; set; }

            public Address[] Addresses { get; set; }
        }

        public class Address
        {
            public string City { get; set; }
        }
    }
}
