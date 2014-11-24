using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_QueryMultiMapTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_OnMultiMapIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_OnMultiMapIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_OnMultiMapIndex_Queries_WithCorrectOffset()
		{
			DoTest(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.FromHours(-10.5)));
		}

		private void DoTest(DateTimeOffset dto)
		{
			using (var documentStore = NewDocumentStore())
			{
				new FoosAndBars_ByDateTimeOffset().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTimeOffset1 = dto });
					session.Store(new Bar { Id = "bars/1", DateTimeOffset2 = dto });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<FoosAndBars_ByDateTimeOffset.Result, FoosAndBars_ByDateTimeOffset>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.DateTimeOffset == dto)
										 .As<object>()
										 .ToList();

					var foo = results.OfType<Foo>().First();
					var bar = results.OfType<Bar>().First();

					Assert.Equal(dto, foo.DateTimeOffset1);
					Assert.Equal(dto, bar.DateTimeOffset2);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTimeOffset DateTimeOffset1 { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public DateTimeOffset DateTimeOffset2 { get; set; }
		}

		public class FoosAndBars_ByDateTimeOffset : AbstractMultiMapIndexCreationTask<FoosAndBars_ByDateTimeOffset.Result>
		{
			public class Result
			{
				public DateTimeOffset DateTimeOffset { get; set; }
			}

			public FoosAndBars_ByDateTimeOffset()
			{
				AddMap<Foo>(foos => from foo in foos
									select new { DateTimeOffset = foo.DateTimeOffset1 });

				AddMap<Bar>(bars => from bar in bars
									select new { DateTimeOffset = bar.DateTimeOffset2 });
			}
		}
	}
}
