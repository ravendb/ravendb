using System;
using System.Globalization;
using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingDateTime : RavenTest
	{
		[Fact]
		public void QueryingNonUtcTime()
		{
			using (var store = NewDocumentStore())
			{
				var dateTime1 = DateTime.ParseExact("2011-04-08T22:00:00.0000000+02:00", new[] { "r", "o" }, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
				var dateTime2 = dateTime1.AddHours(-2);
				using (var s = store.OpenSession())
				{
					s.Store(new Foo
					{
						UtcTime = dateTime1
					});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					Assert.Equal(1, s.Query<Foo>().Where(x => x.UtcTime >= dateTime2).Count());
					Assert.Equal(1, s.Query<Foo>().Where(x => x.UtcTime >= dateTime1).Count());
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTime UtcTime { get; set; }
		}
	}
}
