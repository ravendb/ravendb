using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_QueryMapReduceNotAnalyzedTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_OnMapReduceNotAnalyzedIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_OnMapReduceNotAnalyzedIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_OnMapReduceNotAnalyzedIndex_Queries_WithCorrectOffset()
		{
			DoTest(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.FromHours(-10.5)));
		}

		private void DoTest(DateTimeOffset dto)
		{
			using (var documentStore = NewDocumentStore())
			{
				new Foos_MinAndMaxDateTimeOffset().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTimeOffset = dto.AddDays(-1) });
					session.Store(new Foo { Id = "foos/2", DateTimeOffset = dto });
					session.Store(new Foo { Id = "foos/3", DateTimeOffset = dto.AddDays(1) });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Foos_MinAndMaxDateTimeOffset.Result, Foos_MinAndMaxDateTimeOffset>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .First();

					Assert.Equal(dto.AddDays(-1), results.MinDateTimeOffset);
					Assert.Equal(dto.AddDays(1), results.MaxDateTimeOffset);

					// When not analyzed, I should get be the original offset
					Assert.Equal(dto.Offset, results.MinDateTimeOffset.Offset);
					Assert.Equal(dto.Offset, results.MaxDateTimeOffset.Offset);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTimeOffset DateTimeOffset { get; set; }
		}

		public class Foos_MinAndMaxDateTimeOffset : AbstractIndexCreationTask<Foo, Foos_MinAndMaxDateTimeOffset.Result>
		{
			public class Result
			{
				public DateTimeOffset MinDateTimeOffset { get; set; }
				public DateTimeOffset MaxDateTimeOffset { get; set; }
			}

			public Foos_MinAndMaxDateTimeOffset()
			{
				Map = foos => from foo in foos
							  select new
							  {
								  MinDateTimeOffset = foo.DateTimeOffset,
								  MaxDateTimeOffset = foo.DateTimeOffset
							  };

				Reduce = results => from result in results
									group result by 0
										into g
										select new
										{
											MinDateTimeOffset = g.Min(x => x.MinDateTimeOffset),
											MaxDateTimeOffset = g.Max(x => x.MaxDateTimeOffset)
										};

				Index(x => x.MinDateTimeOffset, FieldIndexing.NotAnalyzed);
				Index(x => x.MaxDateTimeOffset, FieldIndexing.NotAnalyzed);
			}
		}
	}
}