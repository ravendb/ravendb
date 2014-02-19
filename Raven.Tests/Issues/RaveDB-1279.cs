// -----------------------------------------------------------------------
//  <copyright file="RaveDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RaveDB_1279 : RavenTest
    {
        public class Order
        {
            public string Company;
        }

        public class Company
        {
        }

        [Fact]
        public void CanLoadWithoutClrType()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject(), new RavenJObject());
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{Company = "companies/1"});
                    session.SaveChanges();
                }

                using (var sesion = store.OpenSession())
                {
                    var company = sesion.Load<Company>("companies/1");
                    Assert.NotNull(company);
                }
            }
        }

        [Fact]
        public void CanIncludeWithoutClrType()
        {
            using (var store = NewDocumentStore())
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject(), new RavenJObject());
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{Company = "companies/1"});
                    session.SaveChanges();
                }


                using (var sesion = store.OpenSession())
                {
                    var order = sesion.Include<Order>(x => x.Company)
                        .Load("orders/1");
                    var company = sesion.Load<Company>(order.Company);
                    Assert.NotNull(company);
                }
            }
        }
    }
}