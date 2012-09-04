using System;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class DateTimeOffsets : RavenTest
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

		[Fact]
		public void Can_perform_eq_query()
		{
			var dateTimeOffset = DateTimeOffset.Now;

			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new EntityWithNullableDateTimeOffset
					{
						At = dateTimeOffset
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var entityWithNullableDateTimeOffset = s.Query<EntityWithNullableDateTimeOffset>()
						.Where(x => x.At == dateTimeOffset)
						.FirstOrDefault();

					Assert.NotNull(entityWithNullableDateTimeOffset);
				}
			}
		}

		[Fact]
		public void Can_perform_range_query()
		{
			var dateTimeOffset = DateTimeOffset.Now;

			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new EntityWithNullableDateTimeOffset
					{
						At = dateTimeOffset
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					Assert.NotNull(s.Query<EntityWithNullableDateTimeOffset>()
					               	.Where(x => x.At > dateTimeOffset.Subtract(TimeSpan.FromMinutes(5)))
					               	.FirstOrDefault());

					var entityWithNullableDateTimeOffset = s.Query<EntityWithNullableDateTimeOffset>()
						.Where(x => x.At > dateTimeOffset.Add(TimeSpan.FromDays(5)) )
						.FirstOrDefault();
					Assert.Null(entityWithNullableDateTimeOffset);
				}
			}
		}
	}
}
