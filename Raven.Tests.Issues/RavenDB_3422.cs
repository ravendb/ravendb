using System;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3422 
	{
		public class ExampleInt32
		{
			public int Id { get; set; }
		}

		public class ExampleInt64
		{
			public long Id { get; set; }
		}

		[Fact]
		public void CanStoreDocumentWithNonStringIdPropertyAndDefaultValue()

		{
			using (var store = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Configuration =
				{
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
				}

			})

			{
				store.Configuration.Storage.Voron.AllowOn32Bits = true;
				store.Initialize();

				using (var insert = store.BulkInsert())
				{
					Assert.DoesNotThrow(() => insert.Store(new ExampleInt32()));
					Assert.DoesNotThrow(() => insert.Store(new ExampleInt64()));
				}
			}
		}
	}
}
