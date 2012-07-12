// -----------------------------------------------------------------------
//  <copyright file="linmouhong.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class linmouhong : RavenTest
	{
		public class Item
		{
			public Product Product;
		}

		public class Product
		{
			public string Name;
		}
		
		[Fact]
		public void CanCreateProperNestedQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var s = session.Advanced.LuceneQuery<Item>("test").WhereEquals(x => x.Product.Name, "test").ToString();
					
					Assert.Equal("Product_Name:test", s);
					s = session.Advanced.LuceneQuery<Item>().WhereEquals(x => x.Product.Name, "test").ToString();

					Assert.Equal("Product.Name:test", s);
				}
			}
		}
	}
}