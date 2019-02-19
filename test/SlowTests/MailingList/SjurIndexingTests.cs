using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList {
    
    public class SjurIndexingTests : RavenTestBase {

        public class Buyer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string OrgNumber { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
            public string Email { get; set; }
            public string WebsiteUrl { get; set; }
            public bool IsActive { get; set; }
        }

        public class Buyers_Search : AbstractIndexCreationTask<Buyer, Buyers_Search.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public bool IsActive { get; set; }
                public string OrgNumber { get; set; }
                public string Email { get; set; }
                public string Phone { get; set; }
                // Sorting
                public string NameForSorting { get; set; }
            }

            public Buyers_Search()
            {
                Map = item => from t in item
                              select new
                              {
                                  t.Name,
                                  NameForSorting = t.Name,
                                  t.OrgNumber,
                                  t.Phone,
                                  t.Email,
                                  t.IsActive
                              };

                Indexes.Add(x => x.Name, FieldIndexing.Search);
            }
        }

        [Fact]
        public void TestIndexSortWithoutCriteria() {
            var documentStore = GetDocumentStore();
            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                int id = 1;
                do {
                    CreateBuyer(session, id);
                    id++;
                } while (id <= 6);

                session.SaveChanges();
            }

            new Buyers_Search().Execute(documentStore);
            WaitForIndexing(documentStore);

            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .OrderBy(x => x.NameForSorting);

                var buyers = query.OfType<Buyer>().ToList();
                Assert.Equal(6, buyers.Count);
            }

            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                var buyer = session.Load<Buyer>("Buyers/" + 2);
                buyer.Name = "Test Edit";

                session.Store(buyer);
                session.SaveChanges();
            }
            
            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .OrderBy(x => x.NameForSorting);

                var buyers = query.OfType<Buyer>().ToList();
                Assert.Equal(6, buyers.Count);
            }

            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                var buyer = session.Load<Buyer>("Buyers/" + 4);
                buyer.Name = "Test Edit";

                session.Store(buyer);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .OrderByDescending(x => x.NameForSorting);

                var a = query.ToString();

                var buyers = query.OfType<Buyer>().ToList();


                WaitForUserToContinueTheTest(documentStore);

                Assert.Equal(6, buyers.Count);
            }
        }

        [Fact]
        public void TestIndexSortWithCriteria() {
            var documentStore = GetDocumentStore();
            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                int id = 1;
                do {
                    CreateBuyer(session, id);
                    id++;
                } while (id <= 6);

                session.SaveChanges();
            }

            new Buyers_Search().Execute(documentStore);
            WaitForIndexing(documentStore);

            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .Where(x => x.IsActive == true)
                    .OrderBy(x => x.NameForSorting);

                var buyers = query.OfType<Buyer>().ToList();
                Assert.Equal(6, buyers.Count);
            }

            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                var buyer = session.Load<Buyer>("Buyers/" + 2);
                buyer.Name = "Test Edit";

                session.Store(buyer);
                session.SaveChanges();
            }
            
            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .Where(x => x.IsActive == true)
                    .OrderBy(x => x.NameForSorting);

                var buyers = query.OfType<Buyer>().ToList();
                Assert.Equal(6, buyers.Count);
            }

            using (var session = documentStore.OpenSession()) {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                var buyer = session.Load<Buyer>("Buyers/" + 4);
                buyer.Name = "Test Edit";

                session.Store(buyer);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession()) {
                var query = session.Query<Buyers_Search.Result, Buyers_Search>()
                    .Where(x => x.IsActive == true)
                    .OrderByDescending(x => x.NameForSorting);

                var buyers = query.OfType<Buyer>().ToList();
                Assert.Equal(6, buyers.Count);
            }
        }


        private static void CreateBuyer(IDocumentSession session, int id) {
            Buyer buyer = new Buyer() {
                Id = "Buyers/" + id,
                Name = "Buyer " + id,
                IsActive = true,
                Email = "test@test.no",
                OrgNumber = "55555555" + id
            };
            session.Store(buyer);
        }
    }
}
