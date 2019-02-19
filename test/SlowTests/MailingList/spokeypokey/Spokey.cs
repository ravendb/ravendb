using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList.spokeypokey
{
    public class Spokey : RavenTestBase
    {
        private class Employee
        {
            public string FirstName { get; set; }
            public string[] ZipCodes { get; set; }
            public List<string> ZipCodes2 { get; set; }
        }

        [Fact]
        public void Can_query_empty_list()
        {
            var user1 = new Employee() { FirstName = "Joe", ZipCodes2 = new List<string>() };
            var length = user1.ZipCodes2.Count;
            Assert.Equal(0, length);
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {

                    session.Store(user1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    var result = (from u in session.Query<Employee>().Customize(x => x.WaitForNonStaleResults())
                                  where u.ZipCodes2.Count == 0
                                  select u).ToArray();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.Equal(1, result.Length);
                }
            }
        }

        [Fact]
        public void Can_query_empty_array()
        {
            var user1 = new Employee() { FirstName = "Joe", ZipCodes = new string[] { } };
            var length = user1.ZipCodes.Length;
            Assert.Equal(0, length);
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {

                    session.Store(user1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = (from u in session.Query<Employee>().Customize(x => x.WaitForNonStaleResults())
                                  where u.ZipCodes.Length == 0
                                  select u).ToArray();

                    RavenTestHelper.AssertNoIndexErrors(store);
                    Assert.Equal(1, result.Count());
                }
            }
        }

        private class Reference
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
        }


        private class TaxonomyCode : Reference
        {
            public string Code { get; set; }
            public DateTime EffectiveFrom { get; set; }
            public DateTime EffectiveThrough { get; set; }
        }

        private class Provider1
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
            public Reference TaxonomyCodeRef { get; set; }
        }

        private class ProviderTestDto
        {
            public string InternalId { get; set; }
            public string Name { get; set; }
            public TaxonomyCode TaxonomyCode { get; set; }
        }

        private class IdentityProjectionIndex1 : AbstractIndexCreationTask<Provider1>
        {
            public IdentityProjectionIndex1()
            {
                Map =
                    providers => from provider in providers
                                 select new
                                 {
                                     provider.InternalId,
                                     provider.Name,
                                 };

                Store(x => x.InternalId, FieldStorage.Yes);
            }
        }

        [Fact]
        public void Can_project_InternalId_from_transformResults2()
        {
            var taxonomyCode1 = new TaxonomyCode
            {
                EffectiveFrom = new DateTime(2011, 1, 1),
                EffectiveThrough = new DateTime(2011, 2, 1),
                InternalId = "taxonomycodetests/1",
                Name = "ANESTHESIOLOGY",
                Code = "207L00000X",
            };
            var provider1 = new Provider1
            {
                Name = "Joe Schmoe",
                TaxonomyCodeRef = new Reference
                {
                    InternalId = taxonomyCode1.InternalId,
                    Name = taxonomyCode1.Name
                }
            };

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.FindIdentityProperty = (x => x.Name == "InternalId");
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(taxonomyCode1);
                    session.Store(provider1);
                    session.SaveChanges();
                }

                new IdentityProjectionIndex1().Execute(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<Provider1, IdentityProjectionIndex1>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(p => p)
                        .First();

                    Assert.Equal(provider1.Name, result.Name);
                    Assert.Equal(provider1.InternalId, result.InternalId);
                }
            }
        }
    }
}
