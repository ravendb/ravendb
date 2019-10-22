using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanStoreAndGetDateTimeOffset : RavenTestBase
    {
        public CanStoreAndGetDateTimeOffset(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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
