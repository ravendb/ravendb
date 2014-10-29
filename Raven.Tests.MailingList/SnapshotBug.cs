using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class SnapshotBug : RavenTest
	{

		public class TestItem
		{
			public string Id { get; set; }
			public int Count { get; set; }
			public int[] Items { get; set; }
		}

		[Fact]
		public void  CannotModifySnapshotUsingPatch()
		{
			using(var _store = NewDocumentStore())
			{
				var item = new TestItem();
				using (var session = _store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = _store.OpenSession())
				{
					session.Advanced.DocumentStore.DatabaseCommands.Patch(
						item.Id,
						new[] {
                        new PatchRequest() {
                            Type = PatchCommandType.Inc,
                            Name = "Count",
                            Value = RavenJToken.FromObject(1)
                        }
                    });
					session.SaveChanges();
				}
			}
		}

		[Fact]
		public void CannotModifySnapshotUsingPatchNullArray()
		{
			using (var _store = NewDocumentStore())
			{
				var item = new TestItem();
				using (var session = _store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = _store.OpenSession())
				{
					session.Advanced.DocumentStore.DatabaseCommands.Patch(
						item.Id,
						new[] {
                           new PatchRequest() {
                            Type = PatchCommandType.Add,
                            Name = "Items",
                            Value = RavenJToken.FromObject(1)
                        }
                    });
					session.SaveChanges();
				}
			}
		}


		[Fact]
		public void CannotModifySnapshotUsingPatchAddToArray()
		{
			using (var _store = NewDocumentStore())
			{
				var item = new TestItem{Items = new int[]{1,2,3}};
				using (var session = _store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				using (var session = _store.OpenSession())
				{
					session.Advanced.DocumentStore.DatabaseCommands.Patch(
						item.Id,
						new[] {
                        new PatchRequest() {
                            Type = PatchCommandType.Add,
                            Name = "Items",
                            Value = RavenJToken.FromObject(1)
                        }
                    });
					session.SaveChanges();
				}
			}
		} 
	}
}