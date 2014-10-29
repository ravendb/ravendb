using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Issues
{
	public class ReplicationWithReferencedIndexes : ReplicationBase
	{
		public class Item
		{
			public string Id { get; set; }
			public string Ref { get; set; }
			public string Name { get; set; }
		}

		public class Index : AbstractIndexCreationTask<Item>
		{
			public Index()
			{
				Map = items =>
					  from item in items
					  select new
					  {
						  item.Name,
						  RefName = LoadDocument<Item>(item.Ref).Name
					  };
			}
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration serverConfiguration)
		{
			serverConfiguration.RunInMemory = false;
			serverConfiguration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true;
			serverConfiguration.DefaultStorageTypeName = "esent";
		}

		[Fact]
		public void WillReindexReferencesBecauseOfReplication()
		{
			var one = CreateStore();
			var two = CreateStore();

			new Index().Execute(two);

			using (var s1 = one.OpenSession())
			{
				s1.Store(new Item { Id = "items/1", Name = "ayende", Ref = "items/2" });
				s1.SaveChanges();
			}

			// master / master
			TellFirstInstanceToReplicateToSecondInstance();
			TellSecondInstanceToReplicateToFirstInstance();

			WaitForReplication(two, "items/1");

			using (var s2 = two.OpenSession())
			{
                var result = s2.Advanced.DocumentQuery<Item, Index>()
					.WaitForNonStaleResults()
					.WhereEquals("RefName", "rahien")
					.SingleOrDefault();

				Assert.Null(result);
			}
			using (var s1 = one.OpenSession())
			{
				s1.Store(new Item { Id = "items/2", Name = "rahien", Ref = null });
				s1.SaveChanges();
			}
			WaitForReplication(two, "items/2");
			using (var s2 = two.OpenSession())
			{
                var result = s2.Advanced.DocumentQuery<Item, Index>()
					.WaitForNonStaleResults()
					.WhereEquals("RefName", "rahien")
					.SingleOrDefault();

				Assert.Equal("items/1", result.Id);
			}
		}

		[Fact]
		public void WillNotSendDocumentWhenChangedByTouchCount()
		{
			var one = CreateStore();
			var two = CreateStore();

			new Index().Execute(one);

			using (var s1 = one.OpenSession())
			{
				s1.Store(new Item { Id = "items/1", Name = "ayende", Ref = "items/2" });
				s1.SaveChanges();
			}

			// master / master
			TellFirstInstanceToReplicateToSecondInstance();
			TellSecondInstanceToReplicateToFirstInstance();

			WaitForReplication(two, "items/1");

			Etag item1Etag;
			using (var s2 = two.OpenSession())
			{
				item1Etag = s2.Advanced.GetEtagFor(s2.Load<Item>(1));
			}
			using (var s1 = one.OpenSession())
			{
				s1.Store(new Item { Id = "items/2", Name = "rahien", Ref = null });
				s1.SaveChanges();
			}
			using (var s1 = one.OpenSession())
			{
				s1.Query<Item, Index>().Customize(x => x.WaitForNonStaleResults()).ToList();
			}
			WaitForReplication(two, "items/2");
			using (var s1 = one.OpenSession())
			{
				s1.Store(new Item { Id = "items/3", Name = "test", Ref = null });
				s1.SaveChanges();
			}
			WaitForReplication(two, "items/3");
			using (var s2 = two.OpenSession())
			{
				var item1Etag2 = s2.Advanced.GetEtagFor(s2.Load<Item>(1));
				Assert.Equal(item1Etag, item1Etag2);
			}
		}
	}
}