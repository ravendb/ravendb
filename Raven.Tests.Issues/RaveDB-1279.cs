// -----------------------------------------------------------------------
//  <copyright file="RaveDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Linq;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RaveDB_1279 : RavenTest
    {
        public class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }

        public class Company
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanLoadWithoutClrType()
        {
            using (var store = NewRemoteDocumentStore(true))
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject{{"Name","HR"}}, new RavenJObject());
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{CompanyId = "companies/1"});
                    session.SaveChanges();
                }
               
                using (var sesion = store.OpenSession())
                {
                    var company = sesion.Load<Company>("companies/1");
                    Assert.Equal("HR", company.Name);
                }
            }
        }

        [Fact]
        public void CanIncludeWithoutClrType()
        {          

            using (var store = NewRemoteDocumentStore(true))
            {
                store.DatabaseCommands.Put("companies/1", null, new RavenJObject { { "Name", "HR" } }, new RavenJObject());
                using (var session = store.OpenSession())
                {
                    session.Store(new Order{CompanyId = "companies/1"});
                    session.SaveChanges();
                }


                using (var sesion = store.OpenSession())
                {
                    var order = sesion.Include<Order>(x => x.CompanyId)
                                      .Load("orders/1");
                    var company = sesion.Load<Company>(order.CompanyId);
                    Assert.Equal("HR", company.Name);
                    Assert.Equal(1, sesion.Advanced.NumberOfRequests);
                }
            }
        }
    }
}