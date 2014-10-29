using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_QueryStaticTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_OnStaticIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_OnStaticIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_OnStaticIndex_Queries_WithCorrectOffset()
		{
			DoTest(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.FromHours(-10.5)));
		}

		private void DoTest(DateTimeOffset dto)
		{
			using (var documentStore = NewDocumentStore())
			{
				new Foos_ByDateTimeOffset().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTimeOffset = dto });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var foo = session.Query<Foo, Foos_ByDateTimeOffset>()
									 .Customize(x => x.WaitForNonStaleResults())
									 .First(x => x.DateTimeOffset == dto);

					Assert.Equal(dto, foo.DateTimeOffset);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTimeOffset DateTimeOffset { get; set; }
		}

		public class Foos_ByDateTimeOffset : AbstractIndexCreationTask<Foo>
		{
			public Foos_ByDateTimeOffset()
			{
				Map = foos => from foo in foos
							  select new { foo.DateTimeOffset };
			}
		}
	}
}
