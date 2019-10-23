// -----------------------------------------------------------------------
//  <copyright file="RavenDB-1279.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_1279 : RavenTestBase
    {
        public RavenDB_1279(ITestOutputHelper output) : base(output)
        {
        }

        private class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }

        private class Company
        {
            public string Name { get; set; }
        }

        [Fact]
        public void CanLoadWithoutClrType()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
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
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("companies/1", null, new { Name = "HR" }, null);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.SaveChanges();
                }

                using (var sesion = store.OpenSession())
                {
                    var order = sesion.Include<Order>(x => x.CompanyId)
                                      .Load("orders/1-A");
                    var company = sesion.Load<Company>(order.CompanyId);
                    Assert.Equal("HR", company.Name);
                    Assert.Equal(1, sesion.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
