using System;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class GetDocumentUrlOnTransient : RavenTest
	{
		[Fact]
		public void ShouldThrow()
		{
			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" })
			{
				store.Initialize();

				using (IDocumentSession session = store.OpenSession())
				{
					var product = new Product {Name = "First", Cost = 1.1m};

					var invalidOperationException = Assert.Throws<InvalidOperationException>(() => session.Advanced.GetDocumentUrl(product));
					Assert.Equal("Could not figure out identifier for transient instance", invalidOperationException.Message);
				}
			}
		}

		#region Nested type: Product

		public class Product
		{
			public string Name { get; set; }
			public string Id { get; set; }
			public decimal Cost { get; set; }
		}

		#endregion
	}
}