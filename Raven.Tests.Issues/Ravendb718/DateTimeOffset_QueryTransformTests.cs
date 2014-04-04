using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_QueryTransformTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_OnTransformIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_OnTransformIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_OnTransformIndex_Queries_WithCorrectOffset()
		{
			DoTest(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.FromHours(-10.5)));
		}

		private void DoTest(DateTimeOffset dto)
		{
			using (var documentStore = NewDocumentStore())
			{
				new FoosAndBars().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTimeOffset = dto, BarId = "bars/1" });
					session.Store(new Bar { Id = "bars/1", DateTimeOffset = dto });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Foo, FoosAndBars>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.DateTimeOffset == dto)
										 .As<FooAndBar>()
										 .First();

					Assert.Equal(dto, results.FooDateTimeOffset);
					Assert.Equal(dto, results.BarDateTimeOffset);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string BarId { get; set; }
			public DateTimeOffset DateTimeOffset { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public DateTimeOffset DateTimeOffset { get; set; }
		}

		public class FooAndBar
		{
			public string FooId { get; set; }
			public string BarId { get; set; }
			public DateTimeOffset FooDateTimeOffset { get; set; }
			public DateTimeOffset BarDateTimeOffset { get; set; }
		}

		public class FoosAndBars : AbstractIndexCreationTask<Foo>
		{
			public FoosAndBars()
			{
				Map = foos => from foo in foos
							  select new
								  {
									  foo.DateTimeOffset,
								  };

				TransformResults = (database, foos) => from foo in foos
													   let bar = database.Load<Bar>(foo.BarId)
													   select new
														   {
															   FooId = foo.Id,
															   BarId = bar.Id,
															   FooDateTimeOffset = foo.DateTimeOffset,
															   BarDateTimeOffset = bar.DateTimeOffset
														   };
			}
		}
	}
}
