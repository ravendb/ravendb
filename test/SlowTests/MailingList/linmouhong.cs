// -----------------------------------------------------------------------
//  <copyright file="linmouhong.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class linmouhong : RavenTestBase
    {
        private class Item
        {
#pragma warning disable 649
            public Product Product;
#pragma warning restore 649
        }

        private class Product
        {
#pragma warning disable 649
            public string Name;
#pragma warning restore 649
        }

        [Fact]
        public void CanCreateProperNestedQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var s = session.Advanced.DocumentQuery<Item>("test").WhereEquals(x => x.Product.Name, "test").ToString();

                    Assert.Equal("Product_Name:test", s);
                    s = session.Advanced.DocumentQuery<Item>().WhereEquals(x => x.Product.Name, "test").ToString();

                    Assert.Equal("Product.Name:test", s);
                }
            }
        }
    }
}
