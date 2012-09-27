using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Database.Server;
using Xunit;
using System.Linq;
using Raven.Database.Queries;

namespace Raven.Tests.Bugs
{
	public class DynamicQuerySorting : RavenTest
	{
		public class GameServer
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		[Fact]
		public void ShouldNotSortStringAsLong()
		{
			using(var store = NewDocumentStore())
			{
				RavenQueryStatistics stats;
				using(var session = store.OpenSession())
				{
					session.Query<GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}

				var indexDefinition = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition(stats.IndexName);
				Assert.Equal(SortOptions.String, indexDefinition.SortOptions["Name"]);
			}
		}

		[Fact]
		public void ShouldNotSortStringAsLongAfterRestart()
		{
			using (var store = NewDocumentStore())
			{
				RavenQueryStatistics stats;
				using (var session = store.OpenSession())
				{
					session.Query<GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}

				var indexDefinition = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition(stats.IndexName);
				Assert.Equal(SortOptions.String, indexDefinition.SortOptions["Name"]);
			}
		}

		[Fact]
		public void ShouldSelectIndexWhenNoSortingSpecified()
		{
			using (var store = NewDocumentStore())
			{
				RavenQueryStatistics stats;
				using (var session = store.OpenSession())
				{
					session.Query<GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}

				CurrentOperationContext.Headers.Value.Clear();
				var documentDatabase = store.DocumentDatabase;
				var findDynamicIndexName = documentDatabase.FindDynamicIndexName("GameServers", new IndexQuery
				{
					SortedFields = new[]
					{
						new SortedField("Name"),
					}
				});

				Assert.Equal(stats.IndexName, findDynamicIndexName);
			}
		}

		[Fact]
		public void ShouldSelectIndexWhenStringSortingSpecified()
		{
			using (var store = NewDocumentStore())
			{
				RavenQueryStatistics stats;
				using (var session = store.OpenSession())
				{
					session.Query<GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}

				CurrentOperationContext.Headers.Value.Clear();
				CurrentOperationContext.Headers.Value.Set("SortHint-Name", "String");
				var documentDatabase = store.DocumentDatabase;
				var findDynamicIndexName = documentDatabase.FindDynamicIndexName("GameServers", new IndexQuery
				{
					SortedFields = new[]
					{
						new SortedField("Name"),
					}
				});

				Assert.Equal(stats.IndexName, findDynamicIndexName);
			}
		}

	}
}