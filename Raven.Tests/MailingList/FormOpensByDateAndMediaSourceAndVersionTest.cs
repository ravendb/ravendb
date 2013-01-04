using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class FormOpensByDateAndMediaSourceAndVersionTest : IDisposable
	{
		private readonly IDocumentStore _documentStore;


		public FormOpensByDateAndMediaSourceAndVersionTest()
		{
			_documentStore = new EmbeddableDocumentStore { RunInMemory = true }.Initialize();
			_documentStore.DatabaseCommands.DisableAllCaching();

			_documentStore.ExecuteIndex(new FormOpensByDateAndMediaSourceAndVersion());
		}

		public void Dispose()
		{
			_documentStore.Dispose();
		}

		[Fact]
		public void ShouldWork()
		{
			InitData();
			using (var session = _documentStore.OpenSession())
			{
				var queryable = session.Query<CountByDateAndMediaSourceAndVersion_MapReduceResult>(
					FormOpensByDateAndMediaSourceAndVersion.INDEX_NAME)
					.Customize(x => x.WaitForNonStaleResults());
				var list = queryable.ToList();
				PrintOutMapReduceResults(list);
				Assert.True(list.Any());
			}
		}

		[Fact]
		public void Should_return_18_after_aggregation()
		{
			InitData();
			IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable = null;
			int value = 0;
			using (var session = _documentStore.OpenSession())
			{
				queryable =
					session.Query<CountByDateAndMediaSourceAndVersion_MapReduceResult>(
						FormOpensByDateAndMediaSourceAndVersion.INDEX_NAME)
						.Customize(x => x.WaitForNonStaleResults());
				value = Aggregate(queryable);
			}
			Assert.Equal(18, value);
		}

		[Fact]
		public void Should_return_12_after_aggregating_all_GOO()
		{
			InitData();
			IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable = null;
			int value = 0;
			using (var session = _documentStore.OpenSession())
			{
				queryable =
					session.Query<CountByDateAndMediaSourceAndVersion_MapReduceResult>(
						FormOpensByDateAndMediaSourceAndVersion.INDEX_NAME)
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.MediaSource == "GOO");
				value = Aggregate(queryable);
			}
			Assert.Equal(12, value);
		}

		[Fact]
		public void Should_return_3_after_aggregating_all_GOO_Version_5_on_20120902()
		{
			InitData();
			IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable = null;
			int value = 0;
			using (var session = _documentStore.OpenSession())
			{
				queryable =
					session.Query<CountByDateAndMediaSourceAndVersion_MapReduceResult>(
						FormOpensByDateAndMediaSourceAndVersion.INDEX_NAME)
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.MediaSource == "GOO")
						.Where(x => x.Version == "5");
				value = Aggregate(queryable, new DateTime(2012, 9, 2, 1, 1, 1));
			}
			Assert.Equal(3, value);
		}

		private static void PrintOutMapReduceResults(
			IEnumerable<CountByDateAndMediaSourceAndVersion_MapReduceResult> mapReduceResults)
		{
			foreach (var mapReduceResult in mapReduceResults)
			{
				Console.WriteLine(string.Format("Date: {0}-{1}-{2}, MediaSource: {3}, Version: {4}, Count: {5}",
												mapReduceResult.Year, mapReduceResult.Month, mapReduceResult.Day,
												mapReduceResult.MediaSource, mapReduceResult.Version,
												mapReduceResult.Count));
			}
		}

		public int Aggregate(IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable)
		{
			return queryable
				.Take(1024)
				.ToList()
				.Sum(x => x.Count);
		}

		public int Aggregate(IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable,
							 DateTime date)
		{
			return queryable
				.Where(mapReduceResult =>
					   mapReduceResult.Year == date.Year
					   && mapReduceResult.Month == date.Month
					   && mapReduceResult.Day == date.Day)
				.Take(1024)
				.ToList()
				.Sum(x => x.Count);
		}

		private IList<CountByDateAndMediaSourceAndVersion_MapReduceResult> GetMapReduceResult(
			IRavenQueryable<CountByDateAndMediaSourceAndVersion_MapReduceResult> queryable,
			DateTime startDate, DateTime endDate)
		{
			return queryable.Where(x => startDate <= x.Date && x.Date < endDate).ToList();
		}

		private void InitData()
		{
			var date0901 = new DateTime(2012, 9, 1, 1, 2, 3);
			var date0902 = new DateTime(2012, 9, 2, 1, 2, 3);
			var date0903 = new DateTime(2012, 9, 3, 1, 2, 3);
			var date0901MetaData = new MetaData
			{
				CreatedDate = date0901,
				UpdatedDate = date0901
			};
			var date0902MetaData = new MetaData
			{
				CreatedDate = date0902,
				UpdatedDate = date0902
			};
			var date0903MetaData = new MetaData
			{
				CreatedDate = date0903,
				UpdatedDate = date0903
			};
			var visitGOO0 = new Visit
			{
				MediaSource = "GOO",
				Version = "0"
			};
			var visitGOO5 = new Visit
			{
				MediaSource = "GOO",
				Version = "5"
			};
			var visitPOO0 = new Visit
			{
				MediaSource = "POO",
				Version = "0"
			};
			var visitPOO5 = new Visit
			{
				MediaSource = "POO",
				Version = "5"
			};
			var list = new List<FormOpen>
			{
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO0
				},

				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitPOO0
				},
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO0
				},
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0901MetaData,
					Visit = visitGOO5
				},

				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitPOO0
				},
				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitPOO5
				},
				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitGOO5
				},
				new FormOpen
				{
					MetaData = date0902MetaData,
					Visit = visitGOO0
				},

				new FormOpen
				{
					MetaData = date0903MetaData,
					Visit = visitPOO5
				},
				new FormOpen
				{
					MetaData = date0903MetaData,
					Visit = visitPOO5
				},
				new FormOpen
				{
					MetaData = date0903MetaData,
					Visit = visitPOO5
				},
				new FormOpen
				{
					MetaData = date0903MetaData,
					Visit = visitGOO0
				},
				new FormOpen
				{
					MetaData = date0903MetaData,
					Visit = visitGOO0
				},
			};

			using (var documentSession = _documentStore.OpenSession())
			{
				foreach (var click in list)
				{
					documentSession.Store(click);
				}
				documentSession.SaveChanges();
			}
		}


		public class FormOpensByDateAndMediaSourceAndVersion :
			AbstractMultiMapIndexCreationTask
				<CountByDateAndMediaSourceAndVersion_MapReduceResult>
		{
			public const string INDEX_NAME = "FormOpenByDateAndMediaSourceAndVersion";

			public override string IndexName
			{
				get { return INDEX_NAME; }
			}

			public FormOpensByDateAndMediaSourceAndVersion()
			{
				AddMap<FormOpen>(
					objs =>
					from obj in objs
					select new
					{
						Date = obj.MetaData.CreatedDate.Date,
						Year = obj.MetaData.CreatedDate.Year,
						Month = obj.MetaData.CreatedDate.Month,
						Day = obj.MetaData.CreatedDate.Day,
						MediaSource = (string)obj.Visit.MediaSource,
						Version = (string)obj.Visit.Version,
						Count = 1
					}
					);
				Reduce =
					results =>
					from result in results
					group result by
						new { result.Date, result.Year, result.Month, result.Day, result.MediaSource, result.Version }
						into agg
						select
							new
							{
								Date = agg.Key.Date,
								Year = agg.Key.Year,
								Month = agg.Key.Month,
								Day = agg.Key.Day,
								MediaSource = agg.Key.MediaSource,
								Version = agg.Key.Version,
								Count = agg.Sum(x => x.Count)
							};
			}
		}

		public class CountByDateAndMediaSourceAndVersion_MapReduceResult
		{
			public DateTime Date { get; set; }
			public int Year { get; set; }
			public int Month { get; set; }
			public int Day { get; set; }
			public string MediaSource { get; set; }
			public string Version { get; set; }
			public int Count { get; set; }
		}

		public class FormOpen
		{
			public string Id { get; set; }
			public MetaData MetaData { get; set; }

			public Visit Visit { get; set; }
		}

		public class MetaData
		{
			public string Version { get; set; }
			public DateTime CreatedDate { get; set; }
			public DateTime UpdatedDate { get; set; }
			public DateTime? DeletedDate { get; set; }

			public MetaData()
			{
			}

			public MetaData(MetaData metaData)
			{
				if (metaData == null) return;
				Version = metaData.Version;
				CreatedDate = metaData.CreatedDate;
				UpdatedDate = metaData.UpdatedDate;
				DeletedDate = metaData.DeletedDate;
			}
		}

		public class Visit
		{
			public string Id { get; set; }
			public MetaData MetaData { get; set; }

			public string Version { get; set; }
			public string MediaSource { get; set; }
		}
	}
}