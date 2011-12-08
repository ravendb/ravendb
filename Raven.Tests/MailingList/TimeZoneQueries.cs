using System;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class TimeZoneQueries : RavenTest
	{
		public class Item
		{
			public DateTimeOffset At { get; set; }
		}

		[Fact]
		public void CanQueryAtSpecificTimeZone()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						At = new DateTimeOffset(new DateTime(2011,11,11,11,0,0),TimeSpan.FromHours(3))
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<Item>()		// exact match, differnt timezone
						.Where(x => x.At == new DateTimeOffset(new DateTime(2011, 11, 11, 10, 0, 0), TimeSpan.FromHours(2))));

					Assert.NotEmpty(session.Query<Item>()		// greater than equal match, same timezone
						.Where(x => x.At > new DateTimeOffset(new DateTime(2011, 11, 11, 10, 0, 0), TimeSpan.FromHours(3))));

					Assert.NotEmpty(session.Query<Item>()		// less than match, different timezone
						.Where(x => x.At < new DateTimeOffset(new DateTime(2011, 11, 11, 10, 0, 0), TimeSpan.FromHours(-9)))); 
				}
			}
		}
	}
}