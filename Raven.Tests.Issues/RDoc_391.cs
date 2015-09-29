// -----------------------------------------------------------------------
//  <copyright file="RDoc_391.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RDoc_391 : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
		}

		private class People_By_Name_Different : AbstractIndexCreationTask<Person>
		{
			public override string IndexName
			{
				get
				{
					return "People/By/Name";
				}
			}

			public People_By_Name_Different()
			{
				Map = persons => from person in persons select new { person.Name, Count = 1 };
			}
		}

		private class People_By_Name : AbstractIndexCreationTask<Person>
		{
			public People_By_Name()
			{
				Map = persons => from person in persons select new { person.Name };
			}
		}

		private class People_By_Name_With_Scripts : AbstractScriptedIndexCreationTask<Person>
		{
			public override string IndexName
			{
				get
				{
					return "People/By/Name";
				}
			}

			public People_By_Name_With_Scripts()
			{
				Map = persons => from person in persons select new { person.Name };

				IndexScript = @"index";

				DeleteScript = @"delete";

				RetryOnConcurrencyExceptions = false;
			}
		}

		[Fact]
		public void GetIndexStatistics_should_not_advance_last_indexed_etag()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var index = new People_By_Name_With_Scripts();
				index.Execute(store);
				WaitForIndexing(store);
				var statsBefore = store.DatabaseCommands.GetStatistics();
				var indexStats = statsBefore.Indexes.First(x => x.Name == index.IndexName);
				var lastIndexedEtag = indexStats.LastIndexedEtag;

				var statsAfter = store.DatabaseCommands.GetStatistics();
				indexStats = statsAfter.Indexes.First(x => x.Name == index.IndexName);
				Assert.Equal(lastIndexedEtag, indexStats.LastIndexedEtag);
			}
		}


		[Fact]
		public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocument1()
		{
			using (var store = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(People_By_Name_With_Scripts))), store);

				var index = new People_By_Name_With_Scripts();
				var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);

				using (var session = store.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocument2()
		{
			using (var store = NewDocumentStore())
			{
				var index = new People_By_Name_With_Scripts();
				index.Execute(store);

				var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);

				using (var session = store.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocumentOnShardedStore1()
		{
			using (var store1 = NewDocumentStore())
			using (var store2 = NewDocumentStore())
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			                                                              {
				                                                              { "Shard1", store1 },
																			  { "Shard2", store2 },
			                                                              })))
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(People_By_Name_With_Scripts))), store);

				var index = new People_By_Name_With_Scripts();
				var indexDefinition = store1.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);
				indexDefinition = store2.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);

				using (var session = store1.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}

				using (var session = store2.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillCreateIndexAndDocumentOnShardedStore2()
		{
			using (var store1 = NewDocumentStore())
			using (var store2 = NewDocumentStore())
			using (var store = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			                                                              {
				                                                              { "Shard1", store1 },
																			  { "Shard2", store2 },
			                                                              })))
			{
				var index = new People_By_Name_With_Scripts();
				index.Execute(store);

				var indexDefinition = store1.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);
				indexDefinition = store2.DatabaseCommands.GetIndex(index.IndexName);
				Assert.NotNull(indexDefinition);

				using (var session = store1.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}

				using (var session = store2.OpenSession())
				{
					var indexDocument = session.Load<ScriptedIndexResults>(ScriptedIndexResults.IdPrefix + index.IndexName);
					Assert.NotNull(indexDocument);
					Assert.Equal(index.DeleteScript, indexDocument.DeleteScript);
					Assert.Equal(index.IndexScript, indexDocument.IndexScript);
					Assert.Equal(index.RetryOnConcurrencyExceptions, indexDocument.RetryOnConcurrencyExceptions);
					Assert.Equal(ScriptedIndexResults.IdPrefix + index.IndexName, indexDocument.Id);
				}
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillResetIndexIfDocumentIsMissing()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Person
								  {
									  Name = "Name1"
								  });

					session.SaveChanges();
				}

				new People_By_Name().Execute(store);
				var index = new People_By_Name_With_Scripts();

				WaitForIndexing(store);

				var stats = store.DatabaseCommands.GetStatistics();
				var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				Assert.True(EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, Etag.Empty));

				store.DatabaseCommands.Admin.StopIndexing();

				index.Execute(store);

				stats = store.DatabaseCommands.GetStatistics();
				indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				Assert.True(indexStats.LastIndexedEtag.Equals(Etag.Empty));
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillResetIndexIfDocumentHasChanged()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Name1"
					});

					session.SaveChanges();
				}

				new People_By_Name().Execute(store);
				var index = new People_By_Name_With_Scripts();

				store.DatabaseCommands.Put(ScriptedIndexResults.IdPrefix + index.IndexName, null, new RavenJObject(), new RavenJObject());

				WaitForIndexing(store);

				var stats = store.DatabaseCommands.GetStatistics();
				var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				Assert.True(EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, Etag.Empty));

				store.DatabaseCommands.Admin.StopIndexing();

				index.Execute(store);

				stats = store.DatabaseCommands.GetStatistics();
				indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				Assert.True(indexStats.LastIndexedEtag.Equals(Etag.Empty));
			}
		}

		[Fact]
		public void AbstractScriptedIndexCreationTaskWillNotResetIndexIfNothingHasChanged()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Name1"
					});

					session.SaveChanges();
				}


                var index = new People_By_Name_With_Scripts();
				index.Execute(store);

				WaitForIndexing(store);

				var stats = store.DatabaseCommands.GetStatistics();
				var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				var lastIndexedEtag = indexStats.LastIndexedEtag;
				Assert.True(EtagUtil.IsGreaterThan(lastIndexedEtag, Etag.Empty));

				store.DatabaseCommands.Admin.StopIndexing();

				index.Execute(store);

				stats = store.DatabaseCommands.GetStatistics();
				indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
				Assert.True(indexStats.LastIndexedEtag.Equals(lastIndexedEtag) ||
                    EtagUtil.IsGreaterThan(indexStats.LastIndexedEtag, lastIndexedEtag));
			}
		}
	}
}