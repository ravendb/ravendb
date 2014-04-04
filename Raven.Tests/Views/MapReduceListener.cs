// -----------------------------------------------------------------------
//  <copyright file="MapReduceListener.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Indexing;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Xunit;
using Field = Lucene.Net.Documents.Field;

namespace Raven.Tests.Views
{
	public class MapReduceListener : RavenTest
	{
		private EmbeddableDocumentStore store;

		public MapReduceListener()
		{
			store =
				NewDocumentStore(catalog:
					new TypeCatalog(typeof (LoadSecurityDocumentOnMapReducesTrigger),
						typeof (QueryListener),
						typeof (IncludeSecurityFilterFieldsOnIndexTrigger)));
		}

		[Fact]
		public void WhenMapIndexIsCaughtByAListener_CanAddFieldsToIndexedDocuments()
		{
			new SampleItem_SimpleMap().Execute(store);

			using (var session = store.OpenSession())
			{
				var sampleSecurityDocument = new SampleSecurityDocument {Value = "Will filter by this value"};
				session.Store(sampleSecurityDocument);
				session.Store(new SampleItem {Title = "Some title", SecurityDocumentId = sampleSecurityDocument.Id});
				session.SaveChanges();

				var results = session.Advanced.LuceneQuery<SampleItem, SampleItem_SimpleMap>()
					.WaitForNonStaleResults()
					.ToList();
				Assert.NotEmpty(results);
				Assert.NotNull(results.FirstOrDefault(r => r.Title == "Some title"));
			}
		}

		[Fact]
		public void WhenMapReduceIndexIsCaughtByAListener_CanAddFieldsToIndexedDocuments()
		{
			new SampleItem_MapReduce().Execute(store);

			using (var session = store.OpenSession())
			{
				var sampleSecurityDocument = new SampleSecurityDocument {Value = "Will filter by this value"};
				session.Store(sampleSecurityDocument);
				session.Store(new SampleItem {Title = "Some title", SecurityDocumentId = sampleSecurityDocument.Id});
				session.SaveChanges();

				var results = session.Advanced.LuceneQuery<SampleItem_MapReduce.Result, SampleItem_MapReduce>()
					.WaitForNonStaleResults()
					.ToList();
				Assert.NotEmpty(results);
				Assert.NotNull(results.FirstOrDefault(r => r.Title == "Some title"));
			}
		}

		[Fact]
		public void WhenChangingADocumentUsedByATriggerOnTheMapStepOfTheMapReduce_RelatedDocumentsAreReindexedWhenItChanges()
		{
			new SampleItem_MapReduce().Execute(store);
			string secDocId;

			using (var session = store.OpenSession())
			{
				var sampleSecurityDocument = new SampleSecurityDocument {Value = "This Is Not The Correct Value"};
				session.Store(sampleSecurityDocument);
				secDocId = sampleSecurityDocument.Id;

				session.Store(new SampleItem {Title = "Some title", SecurityDocumentId = sampleSecurityDocument.Id});
				session.SaveChanges();

				var results = session.Advanced.LuceneQuery<SampleItem_MapReduce.Result, SampleItem_MapReduce>()
					.WaitForNonStaleResults()
					.ToList();
				Assert.Empty(results);
			}

			using (var session = store.OpenSession())
			{
				var doc = session.Load<SampleSecurityDocument>(secDocId);
				doc.Value = "Will filter by this value";
				session.SaveChanges();

				var results = session.Advanced.LuceneQuery<SampleItem_MapReduce.Result, SampleItem_MapReduce>()
					.WaitForNonStaleResults()
					.ToList();
				Assert.NotEmpty(results);
				Assert.NotNull(results.FirstOrDefault(r => r.Title == "Some title"));
			}
		}

		private class SampleItem_SimpleMap : AbstractIndexCreationTask<SampleItem>
		{
			public SampleItem_SimpleMap()
			{
				Map = items => from item in items
					select new
					{
						item.Title,
						item.SecurityDocumentId,
					};
			}
		}

		private class IncludeSecurityFilterFieldsOnIndexTrigger : AbstractIndexUpdateTrigger
		{
			private class IncludeSecurityFilterFieldsOnIndexTriggerBatcher : AbstractIndexUpdateTriggerBatcher
			{
				private readonly DocumentDatabase database;

				public IncludeSecurityFilterFieldsOnIndexTriggerBatcher(DocumentDatabase database)
				{
					this.database = database;
				}

				public override void OnIndexEntryCreated(string entryKey, Lucene.Net.Documents.Document document)
				{
					var securityDocumentId = document.GetField("SecurityDocumentId").StringValue;
					if (CurrentIndexingScope.Current != null)
					{
						var secDoc = CurrentIndexingScope.Current.LoadDocument(securityDocumentId);
						document.Add(new Field("TestField", secDoc.Value.ToString(), Field.Store.NO,
							Field.Index.NOT_ANALYZED));
					}
					else
					{
						var securityDocument = database.Get(securityDocumentId, null);
						var value = securityDocument.DataAsJson.SelectToken("Value").Value<string>();
						document.Add(new Field("TestField", value, Field.Store.NO, Field.Index.NOT_ANALYZED));
					}
				}
			}

			public override AbstractIndexUpdateTriggerBatcher CreateBatcher(string indexName)
			{
				return new IncludeSecurityFilterFieldsOnIndexTriggerBatcher(Database);
			}
		}

		private class LoadSecurityDocumentOnMapReducesTrigger : AbstractMapOnMapReduceTrigger
		{
			private class LoadSecurityDocumentOnMapReducesTriggerBatcher : AbstractMapOnMapReduceTriggerBatcher
			{
				public override void OnObjectMapped(string entryKey, RavenJObject obj)
				{
					if (obj.Value<string>("SecurityDocumentId") == null) return;

					var securityDocumentId = obj.Value<string>("SecurityDocumentId");
					var secDoc = CurrentIndexingScope.Current.LoadDocument(securityDocumentId);

					obj.Add("TestField", new RavenJValue(secDoc.Value.ToString()));
				}
			}

			public override AbstractMapOnMapReduceTriggerBatcher CreateBatcher(string indexName)
			{
				return new LoadSecurityDocumentOnMapReducesTriggerBatcher();
			}
		}

		private class QueryListener : AbstractIndexQueryTrigger
		{
			public override Query ProcessQuery(string indexName, Query query, IndexQuery originalQuery)
			{
				var allowedQuery = new BooleanQuery
				{
					new BooleanClause(new TermQuery(new Term("TestField", "Will filter by this value")), Occur.MUST)
				};

				return new FilteredQuery(query, new QueryWrapperFilter(allowedQuery));
			}
		}


		private class SampleItem_MapReduce : AbstractIndexCreationTask<SampleItem, SampleItem_MapReduce.Result>
		{
			internal class Result
			{
				public string Title { get; set; }
				public string SecurityDocumentId { get; set; }
				public int Count { get; set; }
			}

			public SampleItem_MapReduce()
			{
				Map = items => from item in items
					select new
					{
						item.Title,
						Count = 1,
						item.SecurityDocumentId,
					};
				Reduce = items => from item in items
					group item by item.Title
					into g
					select new
					{
						Title = g.Key,
						Count = g.Sum(i => i.Count),
						SecurityDocumentId = g.Select(i => i.SecurityDocumentId).FirstOrDefault(),
					};
				Index("TestField", FieldIndexing.NotAnalyzed);
			}
		}

		public class SampleItem : ISecuredType
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public string SecurityDocumentId { get; set; }
		}

		public class SampleSecurityDocument
		{
			public string Id { get; set; }
			public string Value { get; set; }
		}

		public interface ISecuredType
		{
			string SecurityDocumentId { get; set; }
		}
	}
}