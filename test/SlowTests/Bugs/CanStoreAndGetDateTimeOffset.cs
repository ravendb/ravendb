using System;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class CanStoreAndGetDateTimeOffset : RavenTestBase
    {
        public CanStoreAndGetDateTimeOffset(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        private void StoreAndGetDateTimeOffsetTest()
        {
            using (var store = GetDocumentStore())
            {
                var expected = new DateTimeOffset(2010, 11, 10, 19, 13, 26, 509, TimeSpan.FromHours(2));
                using (var session = store.OpenSession())
                {
                    session.Store(new FooBar {Foo = expected});
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fooBar = session.Load<FooBar>("foobars/1-A");
                    Assert.Equal(expected, fooBar.Foo);
                }
            }
        }

        private class FooBar
        {
            public DateTimeOffset Foo { get; set; }
        }
    }
}
