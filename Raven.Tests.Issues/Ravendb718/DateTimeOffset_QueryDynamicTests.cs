using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_QueryDynamicTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_OnDynamicIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_OnDynamicIndex_Queries_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_OnDynamicIndex_Queries_WithCorrectOffset()
		{
			DoTest(new DateTimeOffset(2012, 1, 1, 0, 0, 0, TimeSpan.FromHours(-10.5)));
		}

		private void DoTest(DateTimeOffset dto)
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTimeOffset = dto });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var foo = session.Query<Foo>()
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
	}
}
