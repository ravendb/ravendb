// -----------------------------------------------------------------------
//  <copyright file="SelectDictionaryItem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class SelectDictionaryItem : RavenNewTestBase
    {
        [Fact]
        public void SupportProjectionOnDictionaryField()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Properties = new Dictionary<string, string>
                        {
                            {"Vendor", "Hibernating Rhinos"},
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vendor = session.Query<Product>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(product => product.Properties["Vendor"])
                        .FirstOrDefault();

                    Assert.Equal("Hibernating Rhinos", vendor);
                }
            }
        }

        private class Product
        {
            public string Id { get; set; }
            public Dictionary<string, string> Properties { get; set; }
        }
    }
}
