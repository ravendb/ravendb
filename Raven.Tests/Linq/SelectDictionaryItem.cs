// -----------------------------------------------------------------------
//  <copyright file="SelectDictionaryItem.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
    public class SelectDictionaryItem :RavenTest
    {
        [Fact]
        public void SupportProjectionOnDictionaryField()
        {
            using (var store = NewDocumentStore())
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
                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var vendor = session.Query<Product>().Select(product => product.Properties["Vendor"]).FirstOrDefault();
                    Assert.Equal("Hibernating Rhinos", vendor);
                    
                }
            }
        }

        public class Product
        {
            public int Id { get; set; }
            public Dictionary<string, string> Properties { get; set; }
        } 
    }
}