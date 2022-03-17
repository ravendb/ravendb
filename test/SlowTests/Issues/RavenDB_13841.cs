using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13841 : RavenTestBase
    {
        public RavenDB_13841(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Seed(store);
                Test(store);
            }
        }

        private void Seed(IDocumentStore store)
        {
            new Ent1Index().Execute(store);
            new Ent1Index_Dynamic().Execute(store);

            using (var session = store.OpenSession())
            {
                var ent2 = new Ent2 { Color = "red" };
                session.Store(ent2);

                session.Store(new Ent1 { IdEnt2 = ent2.Id, Description = "abc" });
                session.Store(new Ent1 { IdEnt2 = ent2.Id, Description = "cde" });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);
        }

        private static void Test(IDocumentStore store)
        {
            using (var session = store.OpenSession(new SessionOptions
            {
                NoCaching = true
            }))
            {
                var correct = (from r in session.Query<Ent1Index.Result, Ent1Index>()
                               select new
                               {
                                   r.Description
                               }).ToArray();

                Assert.Equal(2, correct.Length);
                Assert.StartsWith("pre-", correct[0].Description);
                Assert.StartsWith("pre-", correct[1].Description);

                var correct2 = (from r in session.Query<Ent1Index_Dynamic.Result, Ent1Index_Dynamic>()
                           select new
                           {
                               Description = r.Description,
                               DescriptionNoStorage = r.DescriptionNoStorage
                           }).ToArray();

                Assert.Equal(2, correct2.Length);
                Assert.StartsWith("pre-", correct2[0].Description);
                Assert.StartsWith("pre-", correct2[1].Description);
                Assert.Null(correct2[0].DescriptionNoStorage);
                Assert.Null(correct2[1].DescriptionNoStorage);
            }

            using (var session = store.OpenSession(new SessionOptions
            {
                NoCaching = true
            }))
            {
                var incorrect = (from r in session.Query<Ent1Index.Result, Ent1Index>()
                                 let ent2 = RavenQuery.Load<Ent2>(r.IdEnt2) // problem
                                 select new
                                 {
                                     ent2.Color,
                                     r.Description // expected pre-xxx
                                 }).ToArray();

                Assert.Equal(2, incorrect.Length);
                Assert.StartsWith("pre-", incorrect[0].Description);
                Assert.StartsWith("pre-", incorrect[1].Description);

                var incorrect2 = (from r in session.Query<Ent1Index_Dynamic.Result, Ent1Index_Dynamic>()
                                  let ent2 = RavenQuery.Load<Ent2>(r.IdEnt2) // problem
                                  select new
                                  {
                                      ent2.Color,
                                      r.Description, // expected pre-xxx
                                      r.DescriptionNoStorage
                                  }).ToArray();

                Assert.Equal(2, incorrect2.Length);
                Assert.StartsWith("pre-", incorrect2[0].Description);
                Assert.StartsWith("pre-", incorrect2[1].Description);
                Assert.Null(incorrect2[0].DescriptionNoStorage);
                Assert.Null(incorrect2[1].DescriptionNoStorage);
            }
        }

        private class Ent1
        {
            public string Id { get; set; }

            public string IdEnt2 { get; set; }

            public string Description { get; set; }
        }

        private class Ent2
        {
            public string Id { get; set; }

            public string Color { get; set; }
        }

        private class Ent1Index : AbstractIndexCreationTask<Ent1>
        {
            public Ent1Index()
            {
                Map = ents => (from x in ents
                               select new Result
                               {
                                   IdEnt2 = x.IdEnt2,
                                   Description = $"pre-{x.Description}"
                               });

                StoreAllFields(FieldStorage.Yes);
            }

            public class Result
            {
                public string IdEnt2 { get; set; }

                public string Description { get; set; }
            }
        }

        private class Ent1Index_Dynamic : AbstractIndexCreationTask<Ent1>
        {
            public Ent1Index_Dynamic()
            {
                Map = ents => (from x in ents
                               select new
                               {
                                   _ = CreateField("IdEnt2", x.IdEnt2, new CreateFieldOptions { Storage = FieldStorage.Yes }),
                                   __ = CreateField("Description", $"pre-{x.Description}", new CreateFieldOptions { Storage = FieldStorage.Yes }),
                                   ___ = CreateField("DescriptionNoStorage", $"pre-{x.Description}", new CreateFieldOptions { Storage = FieldStorage.No }),
                               });
            }

            public class Result
            {
                public string IdEnt2 { get; set; }

                public string Description { get; set; }

                public string DescriptionNoStorage { get; set; }
            }
        }
    }
}
