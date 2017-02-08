// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4142.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB4142 : RavenNewTestBase
    {
        private class Orders_Triggers : AbstractScriptedIndexCreationTask<Order, Orders_Triggers.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Company { get; set; }
            }

            public Orders_Triggers()
            {
                Map = orders =>
                    from order in orders
                    select new
                    {
                        order.Id,
                        order.Company
                    };

                Reduce = results =>
                    from r in results
                    group r by r.Id
                    into grp
                    select new
                    {
                        grp.First().Id,
                        grp.First().Company
                    };

                IndexScript = @"
                var company = LoadDocument(this.Company);
                if(!company.HasOrders) {
                    company.HasOrders = true;
                    PutDocument(this.Company, company);
                }
            ";
            }
        }
        private class Company
        {
            public Company()
            {
                HasOrders = false;
            }

            public string Id { get; set; }
            public string Name { get; set; }
            public bool HasOrders { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public List<Line> Lines { get; set; }
        }

        private class Line
        {
            public int Quantity { get; set; }
            public int PricePerUnit { get; set; }
            public int Discount { get; set; }
        }


        private static void CreateIndexes(IDocumentStore store)
        {
            new Orders_Triggers().Execute(store);
            //new Companies_ByName().Execute(store.DatabaseCommands, store.Conventions);
        }

        private static Company CreateCompany(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var company = new Company
                {
                    Id = "Companies/",
                    Name = "Microsoft"
                };
                session.Store(company);
                session.SaveChanges();

                return company;
            }
        }

        private static void CreateTwoOrders(IDocumentStore store, string companyId)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Order
                {
                    Id = "Orders/",
                    Company = companyId,
                    Lines = new List<Line>
                    {
                        new Line { Quantity = 1, Discount = 0, PricePerUnit = 10 },
                        new Line { Quantity = 10, Discount = 1, PricePerUnit = 12 },
                        new Line { Quantity = 15, Discount = 2, PricePerUnit = 13 },
                        new Line { Quantity = 6, Discount = 0, PricePerUnit = 20 },
                    }
                });

                session.Store(new Order
                {
                    Id = "Orders/",
                    Company = companyId,
                    Lines = new List<Line>
                    {
                        new Line { Quantity = 12, Discount = 0, PricePerUnit = 10 },
                        new Line { Quantity = 5, Discount = 1, PricePerUnit = 12 },
                        new Line { Quantity = 3, Discount = 2, PricePerUnit = 13 },
                        new Line { Quantity = 1, Discount = 0, PricePerUnit = 20 },
                    }
                });

                session.SaveChanges();
            }
        }

        [Fact(Skip = "RavenDB-5865")]
        public void CanHandlePutAndLoadOnTheSameDocumentInScriptedIndexesInSameBatch()
        {
            //using (var store = GetDocumentStore(activeBundles: "ScriptedIndexResults"))
            using (var store = GetDocumentStore())
            {

                CreateIndexes(store);

                var company = CreateCompany(store);

                Assert.NotNull(company.Id);
                Assert.False(company.HasOrders);

                CreateTwoOrders(store, company.Id);

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                    company = session.Load<Company>(company.Id);

                Assert.True(company.HasOrders, "The company now has orders");
            }
        }

    }
}