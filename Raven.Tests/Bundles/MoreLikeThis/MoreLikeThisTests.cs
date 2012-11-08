using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bundles.MoreLikeThis
{
	public class MoreLikeThisTests : RavenTest
	{
		private readonly IDocumentStore store;

		public MoreLikeThisTests()
		{
			store = NewDocumentStore();
		}

		private static string GetLorem(int numWords)
		{
			const string theLorem = "Morbi nec purus eu libero interdum laoreet Nam metus quam posuere in elementum eget egestas eget justo Aenean orci ligula ullamcorper nec convallis non placerat nec lectus Quisque convallis porta suscipit Aliquam sollicitudin ligula sit amet libero cursus egestas Maecenas nec mauris neque at faucibus justo Fusce ut orci neque Nunc sodales pulvinar lobortis Praesent dui tellus fermentum sed faucibus nec faucibus non nibh Vestibulum adipiscing porta purus ut varius mi pulvinar eu Nam sagittis sodales hendrerit Vestibulum et tincidunt urna Fusce lacinia nisl at luctus lobortis lacus quam rhoncus risus a posuere nulla lorem at nisi Sed non erat nisl Cras in augue velit a mattis ante Etiam lorem dui elementum eget facilisis vitae viverra sit amet tortor Suspendisse potenti Nunc egestas accumsan justo viverra viverra Sed faucibus ullamcorper mauris ut pharetra ligula ornare eget Donec suscipit luctus rhoncus Pellentesque eget justo ac nunc tempus consequat Nullam fringilla egestas leo Praesent condimentum laoreet magna vitae luctus sem cursus sed Mauris massa purus suscipit ac malesuada a accumsan non neque Proin et libero vitae quam ultricies rhoncus Praesent urna neque molestie et suscipit vestibulum iaculis ac nulla Integer porta nulla vel leo ullamcorper eu rhoncus dui semper Donec dictum dui";
			var loremArray = theLorem.Split();
			var output = new StringBuilder();
			var rnd = new Random();

			for (var i = 0; i < numWords; i++)
			{
				output.Append(loremArray[rnd.Next(0, loremArray.Length - 1)]).Append(" ");
			}
			return output.ToString();
		}

		[Fact]
		public void CanGetResults()
		{
			string id;

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var dataQueriedFor = new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"};

				var list = new List<Data>
				{
					dataQueriedFor,
					new Data {Body = "I have a test tomorrow. I hate having a test"},
					new Data {Body = "Cake is great."},
					new Data {Body = "This document has the word test only once"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"}
				};
				list.ForEach(session.Store);
				session.SaveChanges();

				id = session.Advanced.GetDocumentId(dataQueriedFor);
				TestUtil.WaitForIndexing(store);
			}

			AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(id);
		}

		[Fact]
		public void CanCompareDocumentsWithIntegerIdentifiers()
		{
			string id;

			using (var session = store.OpenSession())
			{
				new OtherDataIndex().Execute(store);

				var dataQueriedFor = new DataWithIntegerId {Id = 123, Body = "This is a test. Isn't it great? I hope I pass my test!"};

				var list = new List<DataWithIntegerId>
				{
					dataQueriedFor,
					new DataWithIntegerId {Id = 234, Body = "I have a test tomorrow. I hate having a test"},
					new DataWithIntegerId {Id = 3456, Body = "Cake is great."},
					new DataWithIntegerId {Id = 3457, Body = "This document has the word test only once"},
					new DataWithIntegerId {Id = 3458, Body = "test"},
					new DataWithIntegerId {Id = 3459, Body = "test"},
				};
				list.ForEach(session.Store);
				session.SaveChanges();

				id = session.Advanced.GetDocumentId(dataQueriedFor);

				TestUtil.WaitForIndexing(store);
			}

			Console.WriteLine("Test: '{0}'", id);
			AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(id);

			id = id.ToLower();
			Console.WriteLine("Test with lowercase: '{0}'", id);
			AssetMoreLikeThisHasMatchesFor<DataWithIntegerId, OtherDataIndex>(id);
		}

		[Fact]
		public void CanGetResultsWhenIndexHasSlashInIt()
		{
			const string key = "datas/1";

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
					new Data {Body = "I have a test tomorrow. I hate having a test"},
					new Data {Body = "Cake is great."},
					new Data {Body = "This document has the word test only once"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"}
				};
				list.ForEach(session.Store);
				session.SaveChanges();
				TestUtil.WaitForIndexing(store);
			}

			AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(key);
		}

		[Fact]
		public void Query_On_Document_That_Does_Not_Have_High_Enough_Word_Frequency()
		{
			const string key = "datas/4";

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
					new Data {Body = "I have a test tomorrow. I hate having a test"},
					new Data {Body = "Cake is great."},
					new Data {Body = "This document has the word test only once"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"}
				};
				list.ForEach(session.Store);
				session.SaveChanges();
				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
				{
					DocumentId = key,
					Fields = new[] {"Body"}
				});
				TestUtil.WaitForIndexing(store);

				Assert.Empty(list);
			}
		}

		[Fact]
		public void Test_With_Lots_Of_Random_Data()
		{
			var key = "datas/1";
			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				for (var i = 0; i < 100; i++)
				{
					var data = new Data {Body = GetLorem(200)};
					session.Store(data);
				}
				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			AssetMoreLikeThisHasMatchesFor<Data, DataIndex>(key);
		}

		[Fact]
		public void Do_Not_Pass_FieldNames()
		{
			var key = "datas/1";
			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				for (var i = 0; i < 10; i++)
				{
					var data = new Data {Body = "Body" + i, WhitespaceAnalyzerField = "test test"};
					session.Store(data);
				}
				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

				Assert.NotEmpty(list);
			}
		}

		[Fact]
		public void Each_Field_Should_Use_Correct_Analyzer()
		{
			var key = "datas/1";
			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				for (var i = 0; i < 10; i++)
				{
					var data = new Data {WhitespaceAnalyzerField = "bob@hotmail.com hotmail"};
					session.Store(data);
				}
				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

				Assert.Empty(list);
			}

			key = "datas/11";
			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				for (var i = 0; i < 10; i++)
				{
					var data = new Data {WhitespaceAnalyzerField = "bob@hotmail.com bob@hotmail.com"};
					session.Store(data);
				}
				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(key);

				Assert.NotEmpty(list);
			}
		}

		[Fact]
		public void Can_Use_Min_Doc_Freq_Param()
		{
			const string key = "datas/1";

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
					new Data {Body = "I have a test tomorrow. I hate having a test"},
					new Data {Body = "Cake is great."},
					new Data {Body = "This document has the word test only once"}
				};
				list.ForEach(session.Store);

				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
				{
					DocumentId = key,
					Fields = new[] {"Body"},
					MinimumDocumentFrequency = 2
				});

				Assert.NotEmpty(list);
			}
		}

		[Fact]
		public void Can_Use_Boost_Param()
		{
			const string key = "datas/1";

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. it is a great test. I hope I pass my great test!"},
					new Data {Body = "Cake is great."},
					new Data {Body = "I have a test tomorrow."}
				};
				list.ForEach(session.Store);

				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(
					new MoreLikeThisQuery
					{
						DocumentId = key,
						Fields = new[] {"Body"},
						MinimumWordLength = 3,
						MinimumDocumentFrequency = 1,
						Boost = true
					});

				Assert.NotEqual(0, list.Count());
				Assert.Equal("I have a test tomorrow.", list[0].Body);
			}
		}

		[Fact]
		public void Can_Use_Stop_Words()
		{
			const string key = "datas/1";

			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. Isn't it great? I hope I pass my test!"},
					new Data {Body = "I should not hit this documet. I hope"},
					new Data {Body = "Cake is great."},
					new Data {Body = "This document has the word test only once"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"}
				};
				list.ForEach(session.Store);

				session.Store(new StopWordsSetup {Id = "Config/Stopwords", StopWords = new List<string> {"I", "A", "Be"}});

				session.SaveChanges();

				TestUtil.WaitForIndexing(store);
			}

			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<Data, DataIndex>(new MoreLikeThisQuery
				{
					DocumentId = key,
					StopWordsDocumentId = "Config/Stopwords",
					MinimumDocumentFrequency = 1
				});

				Assert.Equal(5, list.Count());
			}
		}

		private void AssetMoreLikeThisHasMatchesFor<T, TIndex>(string documentKey) where TIndex : AbstractIndexCreationTask, new()
		{
			using (var session = store.OpenSession())
			{
				var list = session.Advanced.MoreLikeThis<T, TIndex>(new MoreLikeThisQuery
				{
					DocumentId = documentKey,
					Fields = new[] {"Body"}
				});

				Assert.NotEmpty(list);
			}
		}

		private void InsertData()
		{
			using (var session = store.OpenSession())
			{
				new DataIndex().Execute(store);

				var list = new List<Data>
				{
					new Data {Body = "This is a test. Isn't it great?"},
					new Data {Body = "I have a test tomorrow. I hate having a test"},
					new Data {Body = "Cake is great."},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"},
					new Data {Body = "test"}
				};

				foreach (var data in list)
				{
					session.Store(data);
				}

				session.SaveChanges();

				//Ensure non stale index
				var testObj = session.Query<Data, DataIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == list[0].Id).SingleOrDefault();
			}
		}

		public class Data
		{
			public string Id { get; set; }
			public string Body { get; set; }
			public string WhitespaceAnalyzerField { get; set; }
		}

		public class DataWithIntegerId
		{
			public long Id;
			public string Body { get; set; }
		}

		public class DataIndex : AbstractIndexCreationTask<Data>
		{
			public DataIndex()
			{
				Map = docs => from doc in docs
				              select new {doc.Body, doc.WhitespaceAnalyzerField};

				Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
				{
					{
						x => x.Body,
						typeof (StandardAnalyzer).FullName
					},
					{
						x => x.WhitespaceAnalyzerField,
						typeof (WhitespaceAnalyzer).FullName
					}
				};

				Stores = new Dictionary<Expression<Func<Data, object>>, FieldStorage>
				{
					{
						x => x.Body, FieldStorage.Yes
					},
					{
						x => x.WhitespaceAnalyzerField, FieldStorage.Yes
					}
				};

			}
		}

		public class OtherDataIndex : AbstractIndexCreationTask<DataWithIntegerId>
		{
			public OtherDataIndex()
			{
				Map = docs => from doc in docs
				              select new {doc.Body};

				Analyzers = new Dictionary<Expression<Func<DataWithIntegerId, object>>, string>
				{
					{
						x => x.Body,
						typeof (StandardAnalyzer).FullName
					}
				};

				Stores = new Dictionary<Expression<Func<DataWithIntegerId, object>>, FieldStorage>
				{
					{
						x => x.Body, FieldStorage.Yes
					}
				};

			}
		}
	}
}