using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23272 : RavenTestBase
    {
        public RavenDB_23272(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        public void CanProjectNestedUnwrappedPropertyFromIndex()
        {
            using var store = GetDocumentStore();
            store.ExecuteIndex(new A_Index());

            using (var session = store.OpenSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.Store(new Parent
                {
                    Name = "A_Name",
                    Son = new Child
                    {
                        Name = "B_Name",
                        Info = "12345"
                    }
                });
                session.SaveChanges();
            }


            using (var session = store.OpenSession())
            {
                var q = session.Query<Parent, A_Index>();
            
                var enumerable = q.Select(a => new { a.Name, Wrapped = new { a.Son.Info } });
                var list = enumerable.ToList();
            
                var fromIndexWrapped = list[0];
                Assert.Equal("12345", fromIndexWrapped.Wrapped.Info);  // ✅
            }

            using (var session = store.OpenSession())
            {
                var q = session.Query<Parent, A_Index>();
                
                var enumerable = q.Select(a => new { a.Name, a.Son.Info });
                var list = enumerable.ToList();
            
                var fromIndexNested = list[0];
                Assert.Equal("12345", fromIndexNested.Info);  // 💥
            }
        }

        private class Parent
        {
            public string Name { get; set; }
            public Child Son { get; set; }
        }

        private class Child
        {
            public string Name { get; set; }
            public string Info { get; set; }
        }


        private class A_Index : AbstractIndexCreationTask<Parent>
        {
            public A_Index()
            {
                Map = a_collection =>
                    from a in a_collection
                    select new
                    {
                        a.Name,
                    };
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        public void CanProjectNestedPropertyFromIndex()
        {
            using var store = GetDocumentStore();
            store.ExecuteIndex(new CompanyIndex());

            using var session = SetupSession(store);

            var fromIndexNestedDeep = session.Query<Company, CompanyIndex>()
                .Select(x => new Target
                {
                    Name = x.Name, 
                    ZipCode = x.Headquarter.VisitAddress.ZipCode
                })
                .Single();
            Assert.Equal("12345", fromIndexNestedDeep.ZipCode); // 💥

            var fromIndexNestedDeep2 = session.Query<Company, CompanyIndex>()
                .Select(x => new
                {
                    x.Name,
                    x.Headquarter.VisitAddress.ZipCode
                })
                .Single();
            Assert.Equal("12345", fromIndexNestedDeep2.ZipCode); // 💥

            var fromIndexNested = session.Query<Company, CompanyIndex>()
                .Select(x => new
                {
                    x.Name,
                    x.Headquarter.VisitAddress
                })
                .Single();
            Assert.Equal("12345", fromIndexNested.VisitAddress?.ZipCode); // 💥

            var fromQuery = session.Query<Company>()
                .Select(x => new
                {
                    x.Name,
                    x.Headquarter.VisitAddress.ZipCode
                })
                .Single();
            Assert.Equal("12345", fromQuery.ZipCode); // ✅

            var fromQuery2 = session.Query<Company>()
                .Select(x => new Target
                {
                    Name = x.Name,
                    ZipCode = x.Headquarter.VisitAddress.ZipCode
                })
                .Single();
            Assert.Equal("12345", fromQuery2.ZipCode); // ✅

            var fromIndexUnnested = session.Query<Company, CompanyIndex>()
                .Select(x => new
                {
                    x.Name,
                    x.Headquarter
                })
                .Single();
            Assert.Equal("12345", fromIndexUnnested.Headquarter.VisitAddress.ZipCode); // ✅

            var fromIndexWrapped = session.Query<Company, CompanyIndex>()
                .Select(x => new
                {
                    x.Name,
                    Wrapped = new { x.Headquarter.VisitAddress.ZipCode }
                })
                .Single();
            Assert.Equal("12345", fromIndexWrapped.Wrapped.ZipCode); // ✅

        }

        private IDocumentSession SetupSession(DocumentStore store)
        {
            var session = store.OpenSession();
            session.Advanced.WaitForIndexesAfterSaveChanges();
            session.Store(new Company
            {
                Name = "HR",
                Headquarter = new()
                {
                    VisitAddress = new()
                    {
                        ZipCode = "12345"
                    },
                    DeliveryAddress = new()
                    {
                        ZipCode = "54321"
                    }
                }
            });
            session.SaveChanges();
            return session;
        }

        private class CompanyIndex : AbstractIndexCreationTask<Company>
        {
            public CompanyIndex()
            {
                Map = companies =>
                    from company in companies
                    select new
                    {
                        company.Name,
                    };
            }
        }

        private class Company
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public Office Headquarter { get; set; }
        }

        private class Office
        {
            public Address VisitAddress { get; set; }
            public Address DeliveryAddress { get; set; }
        }

        private class Address
        {
            public string ZipCode { get; set; }
        }

        private class Target
        {
            public string Name { get; set; }
            public string ZipCode { get; set; }
        }
    }

    
}
