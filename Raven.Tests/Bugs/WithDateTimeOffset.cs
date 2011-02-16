using System;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class WithDateTimeOffset : LocalClientTest
    {
        [Fact]
        public void CanStoreAndGetValues()
        {
            using(var store = NewDocumentStore())
            {
                var expected = new DateTimeOffset(2010, 11, 10, 19, 00, 00, 00, TimeSpan.FromHours(2));
                using (var s = store.OpenSession())
                {
                    s.Store(new FooBar
                    {
                        Foo = expected
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var fooBar = s.Load<FooBar>("foobars/1");
                    Assert.Equal(expected.Offset, fooBar.Foo.Offset);
                }
            }
        }

        public class FooBar
        {
            public DateTimeOffset Foo { get; set; }
        }
    }
}