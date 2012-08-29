using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexDefinitions : RavenTest
	{
		public class MyEntity1
		{
			public string Id { get; set; }
			public string CorpusId { get; set; }
		}

		public class MyEntity2
		{
			public string DocumentId { get; set; }
			public string TopicId { get; set; }
		}

		public class ReduceResult
		{
			public string DocumentId { get; set; }
			public string CorpusId { get; set; }
			public string[] Topics { get; set; }
		}

		public class MyTestIndex1 : AbstractMultiMapIndexCreationTask<ReduceResult>
		{
			public MyTestIndex1()
			{
				AddMap<MyEntity1>(docs => from corpusDoc in docs
				                          select
				                          	new 
				                          		{
				                          			DocumentId = corpusDoc.Id,
				                          			CorpusId = corpusDoc.CorpusId,
				                          			Topics = new string[0]
				                          		}
					);

				AddMap<MyEntity2>(judgments => from j in judgments
				                               select
				                               	new 
				                               		{
				                               			DocumentId = j.DocumentId,
				                               			CorpusId = string.Empty,
				                               			Topics = new string[] {j.TopicId}
				                               		});

				Reduce = results => from result in results
				                    group result by result.DocumentId
				                    into g
				                    select new 
				                           	{
				                           		DocumentId = g.Key,
				                           		CorpusId = g.Select(x => x.CorpusId).FirstOrDefault(),
				                           		Topics = g.SelectMany(x => x.Topics).Distinct().ToArray()
				                           	};
			}
		}

		public class MyTestIndex2 : AbstractMultiMapIndexCreationTask<ReduceResult>
		{
			public MyTestIndex2()
			{
				AddMap<MyEntity1>(docs => from corpusDoc in docs
										  select
											new 
											{
												DocumentId = corpusDoc.Id,
												CorpusId = corpusDoc.CorpusId,
												Topics = (string[])new string[0]
											}
					);

				AddMap<MyEntity2>(judgments => from j in judgments
											   select
												new 
												{
													DocumentId = j.DocumentId,
													CorpusId = string.Empty,
													Topics = new string[] { j.TopicId }
												});

				Reduce = results => from result in results
									group result by result.DocumentId
										into g
										select new 
										{
											DocumentId = g.Key,
											CorpusId = g.Select(x => x.CorpusId).FirstOrDefault(),
											Topics = g.SelectMany(x => x.Topics).Distinct().ToArray()
										};
			}
		}

		public class MyTestIndex3 : AbstractMultiMapIndexCreationTask<ReduceResult>
		{
			public MyTestIndex3()
			{
				AddMap<MyEntity1>(docs => from corpusDoc in docs
				                          select
				                          	new 
				                          		{
				                          			DocumentId = corpusDoc.Id,
				                          			CorpusId = corpusDoc.CorpusId,
				                          			Topics = (string[]) new string[0]
				                          		}
					);

				AddMap<MyEntity2>(judgments => from j in judgments
				                               select
				                               	new 
				                               		{
				                               			DocumentId = j.DocumentId,
				                               			CorpusId = string.Empty,
				                               			Topics = new string[] {j.TopicId}
				                               		});

				Reduce = results => from result in results
				                    group result by result.DocumentId
				                    into g
				                    select new 
				                           	{
				                           		DocumentId = g.Key,
				                           		CorpusId = g.Select(x => x.CorpusId).FirstOrDefault(),
				                           		Topics = g.SelectMany(x => x.Topics).Distinct().ToArray()
				                           	};

				TransformResults = (db, results) => from result in results
				                                    let doc = db.Load<MyEntity1>(result.DocumentId)
				                                    select doc;
			}
		}

		public class MyTestIndex3_UsingNestedClass : AbstractMultiMapIndexCreationTask<MyTestIndex3_UsingNestedClass.MyReduceResult>
		{
			public class MyReduceResult
			{
				public string DocumentId { get; set; }
				public string CorpusId { get; set; }
				public string[] Topics { get; set; }
			}

			public MyTestIndex3_UsingNestedClass()
			{
				AddMap<MyEntity1>(docs => from corpusDoc in docs
										  select
											new 
											{
												DocumentId = corpusDoc.Id,
												CorpusId = corpusDoc.CorpusId,
												Topics = (string[])new string[0]
											}
					);

				AddMap<MyEntity2>(judgments => from j in judgments
											   select
												new 
												{
													DocumentId = j.DocumentId,
													CorpusId = string.Empty,
													Topics = new string[] { j.TopicId }
												});

				Reduce = results => from result in results
									group result by result.DocumentId
										into g
										select new 
										{
											DocumentId = g.Key,
											CorpusId = g.Select(x => x.CorpusId).FirstOrDefault(),
											Topics = g.SelectMany(x => x.Topics).Distinct().ToArray()
										};

				TransformResults = (db, results) => from result in results
													let doc = db.Load<MyEntity1>(result.DocumentId)
													select doc;
			}
		}

		[Fact]
		public void should_compile_index_correctly()
		{
			using (var store = NewDocumentStore())
			{
				new MyTestIndex1().Execute(store);

				new MyTestIndex2().Execute(store);

				new MyTestIndex3().Execute(store);

				new MyTestIndex3_UsingNestedClass().Execute(store);

				var def1 = store.DatabaseCommands.GetIndex(new MyTestIndex1().IndexName);
				Assert.Equal(2, def1.Maps.Count);

				var def2 = store.DatabaseCommands.GetIndex(new MyTestIndex2().IndexName);
				Assert.Equal(2, def2.Maps.Count);

				var def3 = store.DatabaseCommands.GetIndex(new MyTestIndex3().IndexName);
				Assert.Equal(2, def3.Maps.Count);

				var def4 = store.DatabaseCommands.GetIndex(new MyTestIndex3_UsingNestedClass().IndexName);
				Assert.Equal(2, def4.Maps.Count);
			}
		}
	}
}
