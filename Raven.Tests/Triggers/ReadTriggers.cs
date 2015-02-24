//-----------------------------------------------------------------------
// <copyright file="ReadTriggers.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Triggers;

using Xunit;

namespace Raven.Tests.Triggers
{
	public class ReadTriggers : RavenTest
	{
		private readonly DocumentDatabase db;

		public ReadTriggers()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				RunInMemory = true,
				Container = new CompositionContainer(new TypeCatalog(
					typeof(VetoReadsOnCapitalNamesTrigger),
					typeof(HiddenDocumentsTrigger),
					typeof(HideVirtuallyDeletedDocument),
					typeof(UpperCaseNamesTrigger)))
			}, null);
			db.Indexes.PutIndex("ByName",
						new IndexDefinition
						{
							Map = "from doc in docs select new{ doc.name}"
						});
		}

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanFilterAccessToDocumentUsingTrigger_Get()
		{
			db.Documents.Put("abc", null, new RavenJObject(), RavenJObject.Parse("{'name': 'abC'}"), null);

			var jsonDocument = db.Documents.Get("abc", null);

			Assert.Equal("Upper case characters in the 'name' property means the document is a secret!",
				jsonDocument.Metadata.Value<RavenJObject>("Raven-Read-Veto").Value<string>("Reason"));
		}

		[Fact]
		public void CanFilterAccessToDocumentUsingTrigger_GetDocuments()
		{
			db.Documents.Put("abc", null, new RavenJObject(), RavenJObject.Parse("{'name': 'abC'}"), null);

            var jsonDocument = db.Documents.GetDocuments(0, 25, null, CancellationToken.None).First();

			Assert.Equal("Upper case characters in the 'name' property means the document is a secret!",
				jsonDocument.Value<RavenJObject>("@metadata").Value<RavenJObject>("Raven-Read-Veto").Value<string>("Reason"));
		}

		[Fact]
		public void CanFilterAccessToDocumentUsingTrigger_Query()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abC'}"), RavenJObject.Parse("{'name': 'abC'}"), null);
			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					Query = "name:abC",
					PageSize = 10
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal("Upper case characters in the 'name' property means the document is a secret!",
				queryResult.Results[0].Value<RavenJObject>("@metadata").Value<RavenJObject>("Raven-Read-Veto").Value<string>("Reason"));
		}

		[Fact]
		public void CanRemoveFilteredDocumentsFromIndexes()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);
			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					Query = "name:abc",
					PageSize = 10,
					FieldsToFetch = new[] { "__document_id" }
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal(1, queryResult.Results.Count);

			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject { { "Deleted", true } }, null);

			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					Query = "name:abC",
					PageSize = 10,
					FieldsToFetch = new[] { "__document_id" }
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal(0, queryResult.Results.Count);
		}

		[Fact]
		public void CanCompleteHideDocumentUsingTrigger()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), RavenJObject.Parse("{'hidden': true}"), null);

			var jsonDocument = db.Documents.Get("abc", null);

			Assert.Null(jsonDocument);
		}

		[Fact]
		public void CanCompleteHideDocumentUsingTrigger_GetDocuments()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), RavenJObject.Parse("{'hidden': true}"), null);


            Assert.Empty(db.Documents.GetDocuments(0, 25, null, CancellationToken.None));
		}

		[Fact]
		public void CanCompleteHideDocumentUsingTrigger_Query()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), RavenJObject.Parse("{'hidden': true}"), null);
			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					Query = "name:abC",
					PageSize = 10
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Empty(queryResult.Results);
		}


		[Fact]
		public void CanPageThroughFilteredQuery()
		{
			for (int i = 0; i < 15; i++)
			{
				db.Documents.Put(i.ToString(), null, new RavenJObject { { "name", "ayende" } }, new RavenJObject { { "hidden", i % 2 == 0 } }, null);
			}
			db.SpinBackgroundWorkers();

			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					PageSize = 10
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal(7, queryResult.Results.Count);
			Assert.Equal(15, queryResult.TotalResults);
		}

		[Fact]
		public void CanPageThroughFilteredQuery_Page1()
		{
			for (int i = 0; i < 15; i++)
			{
				db.Documents.Put(i.ToString("0000"), null, new RavenJObject { { "name", "ayende" } }, new RavenJObject { { "hidden", i % 2 == 0 } }, null);
			}

			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					PageSize = 3,
					SortedFields = new[] { new SortedField("__document_id"), }
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal(3, queryResult.Results.Count);
			var array = queryResult.Results.Select(x => int.Parse(x["@metadata"].Value<string>("@id"))).OrderBy(x => x).ToArray();
			Assert.Equal(1, array[0]);
			Assert.Equal(3, array[1]);
			Assert.Equal(5, array[2]);
		}

		[Fact]
		public void CanPageThroughFilteredQuery_Page2()
		{
			for (int i = 0; i < 15; i++)
			{
				db.Documents.Put(i.ToString("000"), null, new RavenJObject { { "name", "ayende" } }, new RavenJObject { { "hidden", i % 2 == 0 } }, null);
			}

			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					PageSize = 3,
					SortedFields = new[] { new SortedField("__document_id"), }
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			queryResult = db.Queries.Query("ByName", new IndexQuery
			{
				PageSize = 3,
				Start = queryResult.SkippedResults + queryResult.Results.Count,
				SortedFields = new[] { new SortedField("__document_id"), }
            }, CancellationToken.None);

			Assert.Equal(3, queryResult.Results.Count);
			var array = queryResult.Results.Select(x => int.Parse(x["@metadata"].Value<string>("@id"))).OrderBy(x => x).ToArray();
			Assert.Equal(7, array[0]);
			Assert.Equal(9, array[1]);
			Assert.Equal(11, array[2]);
		}


		[Fact]
		public void CanModifyDocumentUsingTrigger()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			var jsonDocument = db.Documents.Get("abc", null);

			Assert.Equal("ABC", jsonDocument.DataAsJson.Value<string>("name"));
		}

		[Fact]
		public void CanModifyDocumentUsingTrigger_GetDocuments()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);


            Assert.Equal("ABC", db.Documents.GetDocuments(0, 10, null, CancellationToken.None).First().Value<string>("name"));
		}

		[Fact]
		public void CanModifyDocumentUsingTrigger_Query()
		{
			db.Documents.Put("abc", null, RavenJObject.Parse("{'name': 'abc'}"), new RavenJObject(), null);

			db.SpinBackgroundWorkers();
			QueryResult queryResult;
			do
			{
				queryResult = db.Queries.Query("ByName", new IndexQuery
				{
					Query = "name:abC",
					PageSize = 10
                }, CancellationToken.None);
			} while (queryResult.IsStale);

			Assert.Equal("ABC", queryResult.Results[0].Value<string>("name"));
		}

		
	}
}
