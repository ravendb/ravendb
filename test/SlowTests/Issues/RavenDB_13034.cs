using FastTests;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13034 : RavenTestBase
    {
        public RavenDB_13034(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;
            public int Age;
        }

        [Fact]
        public void ExploringConcurrencyBehavior()
        {
            using (var store = GetDocumentStore())
            {
                using (var s1 = store.OpenSession())
                {
                    s1.Store(new User { Name = "Nick", Age = 99 }, "users/1-A");
                    s1.SaveChanges();
                }

                using (var s2 = store.OpenSession())
                {
                    s2.Advanced.UseOptimisticConcurrency = true;

                    var u2 = s2.Load<User>("users/1-A");

                    using (var s3 = store.OpenSession())
                    {
                        var u3 = s3.Load<User>("users/1-A");
                        Assert.NotSame(u2, u3);

                        u3.Age--;
                        s3.SaveChanges();
                    }

                    u2.Age++;

                    var u2_2 = s2.Load<User>("users/1-A");
                    Assert.Same(u2, u2_2);
                    Assert.Equal(1, s2.Advanced.NumberOfRequests);

                    Assert.Throws<ConcurrencyException>(() => s2.SaveChanges());
                    Assert.Equal(2, s2.Advanced.NumberOfRequests);

                    var u2_3 = s2.Load<User>("users/1-A");
                    Assert.Same(u2, u2_3);
                    Assert.Equal(2, s2.Advanced.NumberOfRequests);

                    Assert.Throws<ConcurrencyException>(() => s2.SaveChanges());
                }

                using (var s4 = store.OpenSession())
                {
                    var u4 = s4.Load<User>("users/1-A");
                    Assert.Equal(98, u4.Age);
                }
            }
        }
    }
}
