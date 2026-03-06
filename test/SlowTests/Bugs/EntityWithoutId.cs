using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs
{
    public class EntityWithoutId : RavenTestBase
    {
        public EntityWithoutId(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanBeSaved()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = new User { Name = "Ayende #" + i };
                        s.Store(clone);
                    }
                    s.SaveChanges();
                }


                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Equal("Ayende #" + i, s.Load<User>("users/" + (i + 1)+ "-A").Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanBeUpdated()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = new User { Name = "Ayende #" + i };
                        s.Store(clone);
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = s.Load<User>("users/" + (i + 1)+ "-A");
                        clone.Name = "Rahien #" + i;
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Equal("Rahien #" + i, s.Load<User>("users/" + (i + 1)+ "-A").Name);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanBeDeleted()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = new User { Name = "Ayende #" + i };
                        s.Store(clone);
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = s.Load<User>("users/" + (i + 1)+ "-A");
                        s.Delete(clone);
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Assert.Null(s.Load<User>("users/" + (i + 1)+ "-A"));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CanGetId()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = new User { Name = "Ayende #" + i };
                        s.Store(clone);
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = s.Load<User>("users/" + (i + 1)+ "-A");
                        Assert.Equal("users/" + (i + 1)+ "-A", s.Advanced.GetDocumentId(clone));
                    }
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}