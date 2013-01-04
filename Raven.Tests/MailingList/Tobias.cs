// -----------------------------------------------------------------------
//  <copyright file="Tobias.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Tobias : RavenTest
	{
		[Fact]
		public void CanWork()
		{
			using(var Store = NewDocumentStore())
			{

				new Data_Search().Execute(Store);
				using (var session = Store.OpenSession())
				{
					foreach (var d in data) session.Store(d);
					foreach (var d in mydata) session.Store(d);
					session.SaveChanges();
				}

				using (var session = Store.OpenSession())
				{
					RavenQueryStatistics stats;

					var tst = session.Advanced.LuceneQuery<Data_Search.ReduceResult, Data_Search>()
						.WaitForNonStaleResults()
						.Statistics(out stats)
						.WhereEquals(x => x.Optional, null)
						.SelectFields<dynamic>()
						.ToList();

					Assert.False(stats.IsStale, "Index is stale.");
					Assert.True(tst.Count > 0, "Lucene query for reduce result JObject failed.");

					var tst1 = Queryable.Where(session.Query<Data_Search.ReduceResult, Data_Search>()
							       .Statistics(out stats), x => x.Optional == null)
						.OfType<Data_Search.ProjectionResult>()
						.ToList();

					Assert.False(stats.IsStale, "Index is stale.");
					Assert.True(tst1.Count > 0, "Regular query for projection failed.");

					var tst2 = session.Advanced.LuceneQuery<Data_Search.ReduceResult, Data_Search>()
						.Statistics(out stats)
						.WhereEquals(x => x.Optional, null)
						.SelectFields<Data_Search.ProjectionResult>(new string[0])
						.ToList();

					Assert.False(stats.IsStale, "Index is stale.");
					Assert.True(tst2.Count > 0, "Lucene query for projection failed.");
				}
			}
		}

		private Data_Search.Data[] data = new[]
			{
				new Data_Search.Data { Id = "foo", ParentId = null, Text = "Data 1", TranslatedText = new[] { "Daten 1" }, ParentTranslatedText = new string[0], Num = 1, Type = Data_Search.DataType.Type1, Optional = null },
				new Data_Search.Data { Id = "boo", ParentId = null, Text = "Data 2", TranslatedText = new[] { "Daten 2" }, ParentTranslatedText = new string[0], Num = 2, Type = Data_Search.DataType.Type2, Optional = 1 }
			};

		private Data_Search.MyData[] mydata = new[]
			{
				new Data_Search.MyData { Id = "myfoo", DataId = "foo", Locations = new string[0] },
				new Data_Search.MyData { Id = "myotherfoo", DataId = "foo", Locations = new string[0] }
			};


		public class Data_Search : AbstractMultiMapIndexCreationTask<Data_Search.ReduceResult>
		{
			public enum DataType { Unknown, Type1, Type2 };

			public class Data
			{
				public string Id { get; set; }
				public string ParentId { get; set; }
				public string Text { get; set; }
				public string[] TranslatedText { get; set; }
				public string[] ParentTranslatedText { get; set; }
				public int Num { get; set; }
				public DataType Type { get; set; }
				public int? Optional { get; set; }
			}

			public class MyData
			{
				public string Id { get; set; }
				public string DataId { get; set; }
				public string[] Locations { get; set; }
			}

			public class ReduceResult
			{
				public string DataId { get; set; }
				public string[] MyDataIds { get; set; }
				public string Text { get; set; }
				public string[] TranslatedText { get; set; }
				public int Num { get; set; }
				public DataType Type { get; set; }
				public int? Optional { get; set; }
			}

			public class ProjectionResult
			{
				public Data Data { get; set; }
				public MyData[] MyDatas { get; set; }
			}

			public Data_Search()
			{
				AddMap<MyData>(mds => from md in mds
									  where md.DataId != null
									  select new
									  {
										  DataId = md.DataId,
										  MyDataIds = new string[] { md.Id },
										  Text = (string)null,
										  TranslatedText = new string[0],
										  Num = -999,
										  Type = DataType.Unknown,
										  Optional = -999
									  });
				AddMap<Data>(ds => from d in ds
								   select new
								   {
									   DataId = d.Id,
									   MyDataIds = new string[0],
									   Text = d.Text,
									   TranslatedText = d.TranslatedText.Select(x => x).Concat(d.ParentTranslatedText.Select(x => x)).ToArray(),
									   Num = d.Num,
									   Type = d.Type,
									   Optional = d.Optional
								   });

				Reduce = rs => from r in rs
							   group r by r.DataId into i
							   select new
							   {
								   DataId = i.Key,
								   MyDataIds = i.SelectMany(x => x.MyDataIds).Where(x => x != null).ToArray(),
								   Text = i.Select(x => x.Text).Where(x => x != null).FirstOrDefault(),
								   TranslatedText = i.Select(x => x.TranslatedText).Where(x => x.Length > 0).FirstOrDefault(),
								   Num = i.Select(x => x.Num).Where(x => x != -999).DefaultIfEmpty(-999).First(),
								   Type = i.Select(x => x.Type).Where(x => x != DataType.Unknown).DefaultIfEmpty(DataType.Unknown).First(),
								   Optional = i.Select(x => x.Optional).Where(x => x != -999).DefaultIfEmpty(-999).First()
							   };

				TransformResults = (db, ts) => from t in ts
											   where t.MyDataIds.Count() > 0
											   select new
											   {
												   Data = db.Load<Data>(t.DataId),
												   MyDatas = db.Load<MyData>(t.MyDataIds)
											   };

				Index(x => x.Text, FieldIndexing.Analyzed);
				Index(x => x.TranslatedText, FieldIndexing.Analyzed);

				Analyze(x => x.Text, typeof(SimpleAnalyzer).FullName);
				Analyze(x => x.TranslatedText, typeof(SimpleAnalyzer).FullName);
			}
		}
	
	}
}