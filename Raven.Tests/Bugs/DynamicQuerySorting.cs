using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Linq;
using Raven.Database.Server;
using Raven.Tests.Common;

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

				var indexDefinition = store.SystemDatabase.IndexDefinitionStorage.GetIndexDefinition(stats.IndexName);
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

				var indexDefinition = store.SystemDatabase.IndexDefinitionStorage.GetIndexDefinition(stats.IndexName);
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

                CurrentOperationContext.Headers.Value = new Lazy<NameValueCollection>(() => new NameValueCollection());
				var documentDatabase = store.SystemDatabase;
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

                CurrentOperationContext.Headers.Value = new Lazy<NameValueCollection>(() => new NameValueCollection());
				CurrentOperationContext.Headers.Value.Value.Set("SortHint-Name", "String");
				var documentDatabase = store.SystemDatabase;
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
		public void ShouldSelectIndexWhenStringSortingSpecifiedByUsingQueryString()
		{
			using (var store = NewRemoteDocumentStore())
			{
				RavenQueryStatistics stats;
				using (var session = store.OpenSession())
				{
					session.Query<GameServer>()
						.Statistics(out stats)
						.OrderBy(x => x.Name)
						.ToList();
				}

                CurrentOperationContext.Headers.Value = new Lazy<NameValueCollection>(() => new NameValueCollection());

				var indexQuery = new IndexQuery
								 {
									 SortedFields = new[] { new SortedField("Name") },
									 SortHints = new Dictionary<string, SortOptions> { { "Name", SortOptions.String } }
								 };

				var url = store.Url.ForDatabase(store.DefaultDatabase).Indexes("dynamic/GameServers") + indexQuery.GetQueryString();
				var request = store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, store.DatabaseCommands.PrimaryCredentials, store.Conventions));
				var result = request.ReadResponseJson().JsonDeserialization<QueryResult>();

				Assert.Equal(stats.IndexName, result.IndexName);
			}
		}
	}
}