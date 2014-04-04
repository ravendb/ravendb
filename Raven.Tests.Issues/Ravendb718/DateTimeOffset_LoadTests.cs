using System;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTimeOffset_LoadTests : RavenTest
	{
		[Fact]
		public void DateTimeOffset_FromNow_Loads_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.Now);
		}

		[Fact]
		public void DateTimeOffset_FromUtcNow_Loads_WithCorrectOffset()
		{
			DoTest(DateTimeOffset.UtcNow);
		}

		[Fact]
		public void DateTimeOffset_FromNew_Loads_WithCorrectOffset()
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
					var foo = session.Load<Foo>("foos/1");
					Assert.Equal(dto, foo.DateTimeOffset);
					Assert.Equal(dto.Offset, foo.DateTimeOffset.Offset);
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
