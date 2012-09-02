using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{ /// <summary>
	/// This test demonstrates an exception being thrown when sorting nullable Ints in Dynamic Fields.
	/// "Invalid shift value in prefixCoded string (is encoded value really an INT?)"
	/// 
	/// This is similar to the issue reported by Tobias Sebring that was fixed in Build 2062.
	/// https://groups.google.com/forum/?fromgroups=#!topic/ravendb/DCNn0uT15H4
	/// 
	/// Current Build: 2073
	/// </summary>
	public class NullableExample : RavenTest
	{
		public class Doc
		{
			public string DocId { get; set; }
			public Dictionary<string, object> Map { get; set; }
		}


		
		[Fact]
		public void InvalidShirt()
		{
			using (var store = NewDocumentStore())
			{

				store.Initialize();

				store.DatabaseCommands.Delete("1", null);
				store.DatabaseCommands.Delete("2", null);
				store.DatabaseCommands.Delete("3", null);
				store.DatabaseCommands.Delete("4", null);
				store.DatabaseCommands.Delete("5", null);

				store.DatabaseCommands.DeleteIndex("test");
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { DocId = doc.DocId, _ = doc.Map.Select(p => CreateField(p.Key, p.Value)) }",
					SortOptions = new Dictionary<string, SortOptions> { { "X", SortOptions.Int } }
				});
				using (var session = store.OpenSession())
			{
				// Insert valid documents.
				session.Store(new Doc { DocId = "1", Map = new Dictionary<string, object> { { "X", 0 } } }, "1");
				session.Store(new Doc { DocId = "2", Map = new Dictionary<string, object> { { "X", 1 } } }, "2");
				session.Store(new Doc { DocId = "3", Map = new Dictionary<string, object> { { "X", 2 } } }, "3");
				// Doc 4 has no "X" in it's map.
				session.Store(new Doc { DocId = "4", Map = new Dictionary<string, object>() }, "4");
				session.SaveChanges();

				// Everything works fine.
				var sortedResults = session.Advanced
					.LuceneQuery<Doc>("test")
					.AddOrder("X", true)
					.WaitForNonStaleResults()
					.ToList();

				Assert.NotNull(sortedResults);
				Assert.Equal(4, sortedResults.Count);
				Assert.Equal("3", sortedResults[0].DocId);
				Assert.Equal("2", sortedResults[1].DocId);
				Assert.Equal("1", sortedResults[2].DocId);
				Assert.Equal("4", sortedResults[3].DocId);

				// Doc 5 has a null value for X
				session.Store(new Doc { DocId = "5", Map = new Dictionary<string, object> { { "X", null } } }, "5");
				session.SaveChanges();

				Assert.DoesNotThrow(() =>
				{
					// Invalid shift value in prefixCoded string (is encoded value really an INT?)
					var throwsExceptions = session.Advanced
						.LuceneQuery<Doc>("test")
						.AddOrder("X", true)
						.WaitForNonStaleResults()
						.ToList();

					/*Url: "/indexes/NullableExample?query=&start=0&pageSize=128&aggregation=None&sort=-X"

					System.FormatException: Invalid shift value in prefixCoded string (is encoded value really an INT?)
					at Lucene.Net.Util.NumericUtils.PrefixCodedToInt(String prefixCoded) in z:\Libs\lucene.net\src\core\Util\NumericUtils.cs:line 251
					at Lucene.Net.Search.FieldCacheImpl.IntCache.CreateValue(IndexReader reader, Entry entryKey) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 546
					at Lucene.Net.Search.FieldCacheImpl.Cache.Get(IndexReader reader, Entry key) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 258
					at Lucene.Net.Search.FieldCacheImpl.GetInts(IndexReader reader, String field, IntParser parser) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 511
					at Lucene.Net.Search.FieldCacheImpl.IntCache.CreateValue(IndexReader reader, Entry entryKey) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 533
					at Lucene.Net.Search.FieldCacheImpl.Cache.Get(IndexReader reader, Entry key) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 258
					at Lucene.Net.Search.FieldCacheImpl.GetInts(IndexReader reader, String field, IntParser parser) in z:\Libs\lucene.net\src\core\Search\FieldCacheImpl.cs:line 511
					at Lucene.Net.Search.FieldComparator.IntComparator.SetNextReader(IndexReader reader, Int32 docBase) in z:\Libs\lucene.net\src\core\Search\FieldComparator.cs:line 403
					at Lucene.Net.Search.IndexSearcher.Search(Weight weight, Filter filter, Collector collector) in z:\Libs\lucene.net\src\core\Search\IndexSearcher.cs:line 286
					at Lucene.Net.Search.IndexSearcher.Search(Weight weight, Filter filter, Int32 nDocs, Sort sort, Boolean fillFields) in z:\Libs\lucene.net\src\core\Search\IndexSearcher.cs:line 274
					at Lucene.Net.Search.IndexSearcher.Search(Weight weight, Filter filter, Int32 nDocs, Sort sort) in z:\Libs\lucene.net\src\core\Search\IndexSearcher.cs:line 206
					at Lucene.Net.Search.Searcher.Search(Query query, Filter filter, Int32 n, Sort sort) in z:\Libs\lucene.net\src\core\Search\Searcher.cs:line 107
					at Raven.Database.Indexing.Index.IndexQueryOperation.ExecuteQuery(IndexSearcher indexSearcher, Query luceneQuery, Int32 start, Int32 pageSize, IndexQuery indexQuery) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Indexing\Index.cs:line 1018
					at Raven.Database.Indexing.Index.IndexQueryOperation.<Query>d__41.MoveNext() in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Indexing\Index.cs:line 778
					at System.Linq.Enumerable.WhereSelectEnumerableIterator`2.MoveNext()
					at System.Linq.Enumerable.WhereSelectEnumerableIterator`2.MoveNext()
					at System.Collections.Generic.List`1.InsertRange(Int32 index, IEnumerable`1 collection)
					at Raven.Database.DocumentDatabase.<>c__DisplayClass88.<Query>b__7e(IStorageActionsAccessor actions) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\DocumentDatabase.cs:line 946
					at Raven.Storage.Esent.TransactionalStorage.ExecuteBatch(Action`1 action) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Storage\Esent\TransactionalStorage.cs:line 437
					at Raven.Storage.Esent.TransactionalStorage.Batch(Action`1 action) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Storage\Esent\TransactionalStorage.cs:line 397
					at Raven.Database.DocumentDatabase.Query(String index, IndexQuery query) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\DocumentDatabase.cs:line 951
					at Raven.Database.Server.Responders.Index.PerformQueryAgainstExistingIndex(IHttpContext context, String index, IndexQuery indexQuery, Guid& indexEtag) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Server\Responders\Index.cs:line 386
					at Raven.Database.Server.Responders.Index.ExecuteQuery(IHttpContext context, String index, Guid& indexEtag) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Server\Responders\Index.cs:line 323
					at Raven.Database.Server.Responders.Index.GetIndexQueryRessult(IHttpContext context, String index) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Server\Responders\Index.cs:line 262
					at Raven.Database.Server.HttpServer.DispatchRequest(IHttpContext ctx) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Server\HttpServer.cs:line 685
					at Raven.Database.Server.HttpServer.HandleActualRequest(IHttpContext ctx) in c:\Builds\RavenDB-Unstable-v1.2\Raven.Database\Server\HttpServer.cs:line 447*/
				});
			}}
		}
	}
}