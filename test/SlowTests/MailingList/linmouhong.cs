// -----------------------------------------------------------------------
//  <copyright file="linmouhong.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
#pragma warning disable CS8981 // The type name only contains lower-cased ascii characters. Such names may become reserved for the language.
    public class linmouhong : RavenTestBase
#pragma warning restore CS8981 // The type name only contains lower-cased ascii characters. Such names may become reserved for the language.
    {
        public linmouhong(ITestOutputHelper output) : base(output)
        {
        }

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
                    var s = session.Advanced.DocumentQuery<Item>("test").WhereEquals(x => x.Product.Name, "test").GetIndexQuery();

                    Assert.Equal("from index 'test' where Product_Name = $p0", s.Query);
                    Assert.Equal("test", s.QueryParameters["p0"]);

                    s = session.Advanced.DocumentQuery<Item>().WhereEquals(x => x.Product.Name, "test").GetIndexQuery();

                    Assert.Equal("from 'Items' where Product.Name = $p0", s.Query);
                    Assert.Equal("test", s.QueryParameters["p0"]);
                }
            }
        }
    }
}
