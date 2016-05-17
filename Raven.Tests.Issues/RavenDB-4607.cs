// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4607.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4607 : RavenTest
    {
        public class Company
        {
            public Address Address { get; set; }
        }

        public class Address
        {
            public object Line1 { get; set; }
        }

        [Fact]
        public void can_change_object_to_double()
        {
            using (var store = NewDocumentStore())
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

                    Assert.DoesNotThrow(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void can_change_object_to_decimal()
        {
            using (var store = NewDocumentStore())
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

                    Assert.DoesNotThrow(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void can_change_nested_object_to_double()
        {
            using (var store = NewDocumentStore())
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

                    Assert.DoesNotThrow(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void can_change_nested_object_to_decimal()
        {
            using (var store = NewDocumentStore())
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

                    Assert.DoesNotThrow(() => session.SaveChanges());
                }
            }
        }
    }
}
