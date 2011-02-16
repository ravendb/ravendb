using System;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class WithDateTimeOffsetRemote : RemoteClientTest
    {
        [Fact]
        public void CanStoreAndGetValues()
        {
            using(GetNewServer())
            using (var store = new DocumentStore{Url = "http://localhost:8080"}.Initialize())
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
                    Assert.Equal(expected, fooBar.Foo);
                }
            }
        }

        public class FooBar
        {
            public DateTimeOffset Foo { get; set; }
        }
    }
}