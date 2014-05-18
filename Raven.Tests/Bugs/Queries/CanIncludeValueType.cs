// -----------------------------------------------------------------------
//  <copyright file="CanIncludeValueType.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class CanIncludeValueType : RavenTest, IDisposable
	{
		private readonly IDocumentStore store;
		private readonly RavenDbServer ravenDbServer;

		private class Product
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public int CategoryId { get; set; }
		}

		private class Category
		{
			public int Id { get; set; }
			public string Name { get; set; }
		}

		public CanIncludeValueType()
		{
			ravenDbServer = GetNewServer();
			store = new DocumentStore
			        	{
			        		Url = "http://localhost:8079"
			        	}.Initialize();

			using (var session = store.OpenSession())
			{
				var category = new Category { Name = "Widgets" };
				session.Store(category);

				var product = new Product
				              	{
				              		Name = "Blue Widget",
				              		CategoryId = category.Id
				              	};
				session.Store(product);

				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			store.Dispose();
			ravenDbServer.Dispose();

			base.Dispose();
		}

		[Fact]
		public void CanIncludeBasedOnValueType()
		{
			using (var session = store.OpenSession())
			{
				var product = session.Query<Product>()
					.Customize(x => x.WaitForNonStaleResults())
					.Include<Product, Category>(p => p.CategoryId)
					.Single();
				Assert.NotNull(product);

				var category = session.Load<Category>(product.CategoryId);
				Assert.NotNull(category);

				Assert.Equal(1, session.Advanced.NumberOfRequests);
			}
		}

		[Fact]
		public void WillFailWhenUsingIncludeOnValueTypeWithoutSpecifyingTheIncludeObjectType()
		{
			using (var session = store.OpenSession())
			{
				var ex = Assert.Throws<InvalidOperationException>(() =>
				                                                  	{
				                                                  		var product = session.Query<Product>()
				                                                  			.Customize(x => x.WaitForNonStaleResults())
				                                                  			.Include(p => p.CategoryId)
				                                                  			.Single();
				                                                  	});
				Assert.Equal("You cannot use Include<TResult> on value type. Please use the Include<TResult, TInclude> overload.", ex.Message);
			}
		}

		[Fact]
		public void WillDoARoundtripForAPlainPropertyName()
		{
			using (var session = store.OpenSession())
			{
				var product = session.Query<Product>()
					.Customize(x => x.WaitForNonStaleResults().Include("CategoryId"))
					.Single();
				Assert.NotNull(product);

				var category = session.Load<Category>(product.CategoryId);
				Assert.NotNull(category);

				Assert.Equal(2, session.Advanced.NumberOfRequests);
			}
		}
	}
}