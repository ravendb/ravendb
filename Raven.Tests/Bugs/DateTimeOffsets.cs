using System;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DateTimeOffsets : LocalClientTest
	{
		[Fact]
		public void Can_save_and_load()
		{
			var dateTimeOffset = DateTimeOffset.Now;

			using (var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new EntityWithNullableDateTimeOffset
					{
						At = dateTimeOffset
					});

					s.Store(new EntityWithNullableDateTimeOffset
					{
						At = null
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{

					Assert.Equal(dateTimeOffset, s.Load<EntityWithNullableDateTimeOffset>("EntityWithNullableDateTimeOffsets/1").At);
					Assert.Null(s.Load<EntityWithNullableDateTimeOffset>("EntityWithNullableDateTimeOffsets/2").At);
					s.SaveChanges();
				}
			}
		}
	}

	public class EntityWithNullableDateTimeOffset
	{
		public DateTimeOffset? At { get; set; }
	}
}