// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4607.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4607 : RavenTestBase
    {
        public RavenDB_4607(ITestOutputHelper output) : base(output)
        {
        }

        private class Company
        {
            public Address Address { get; set; }
        }

        private class Address
        {
            public object Line1 { get; set; }
        }

        [Fact]
        public void can_change_object_to_double()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var test = new Address
                    {
                        Line1 = ""
                    };
                    session.Store(test, "address");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Address>("address");
                    company.Line1 = 1.5d;

                    session.SaveChanges(); // does not throw
                }
            }
        }

        [Fact]
        public void can_change_object_to_decimal()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var test = new Address
                    {
                        Line1 = ""
                    };
                    session.Store(test, "address");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Address>("address");
                    company.Line1 = 1.5m;

                    session.SaveChanges(); // does not throw
                }
            }
        }

        [Fact]
        public void can_change_nested_object_to_double()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var test = new Company
                    {
                        Address = new Address
                        {
                            Line1 = "1.5"
                        }
                    };
                    session.Store(test, "company");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("company");
                    company.Address.Line1 = 1.5d;

                    session.SaveChanges(); // does not throw
                }
            }
        }

        [Fact]
        public void can_change_nested_object_to_decimal()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var test = new Company
                    {
                        Address = new Address
                        {
                            Line1 = ""
                        }
                    };
                    session.Store(test, "company");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("company");
                    company.Address.Line1 = 3.5m;

                    session.SaveChanges(); // does not throw
                }
            }
        }
    }
}
