using System;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DateTimeOffsets : RavenTestBase
    {
        public DateTimeOffsets(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_save_and_load()
        {
            var dateTimeOffset = DateTimeOffset.Now;

            using (var store = GetDocumentStore())
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

                    Assert.Equal(dateTimeOffset, s.Load<EntityWithNullableDateTimeOffset>("EntityWithNullableDateTimeOffsets/1-A").At);
                    Assert.Null(s.Load<EntityWithNullableDateTimeOffset>("EntityWithNullableDateTimeOffsets/2-A").At);
                    s.SaveChanges();
                }
            }
        }

        [Fact]
        public void Can_perform_eq_query()
        {
            var dateTimeOffset = DateTimeOffset.Now;

            using (var store = GetDocumentStore())
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

            using (var store = GetDocumentStore())
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

        private class EntityWithNullableDateTimeOffset
        {
            public DateTimeOffset? At { get; set; }
        }
    }
}
