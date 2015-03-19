using System.Collections.Generic;
using System.IO;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3276 : RavenTestBase
	{

		[Fact]
		public void Dictionary_with_empty_string_as_key_should_fail_storing_in_db()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(
						new TestEntity
						{
							Items = new Dictionary<string, string>
							{
								{"", "value for empty string"}
							}
						});

					Assert.Throws<InvalidDataException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void Dictionary_with_empty_string_as_key_should_fail_bulk_insert()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert())
				{
					Assert.Throws<InvalidDataException>(() =>
					bulkInsert.Store(new TestEntity
					{
						Items = new Dictionary<string, string>
						{
							{"", "value for empty string"}
						}
					}));
				}
			}
		}


		class TestEntity
		{
			public string Id { get; set; }
			public Dictionary<string, string> Items { get; set; }
		}
	}
}
