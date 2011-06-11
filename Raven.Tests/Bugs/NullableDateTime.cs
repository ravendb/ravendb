using System;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class NullableDateTime : LocalClientTest
	{
		[Fact]
		public void WillNotIncludeItemsWithNullDate()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new WithNullableDateTime());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var withNullableDateTimes = session.Query<WithNullableDateTime>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.CreatedAt > new DateTime(2000, 1, 1) && x.CreatedAt != null)
						.ToList();
					Assert.Empty(withNullableDateTimes);
				}
			}
		}

		public class WithNullableDateTime
		{
			public string Id { get; set; }
			public DateTime? CreatedAt { get; set; }
		}
	}
}