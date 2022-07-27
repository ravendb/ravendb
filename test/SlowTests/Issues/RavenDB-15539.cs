using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Newtonsoft.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15539 : RavenTestBase
    {
        public RavenDB_15539(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name;

            [JsonIgnore]
            public bool IgnoreChanges;
        }

        [Fact]
        public void CanIgnoreChanges()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.ShouldIgnoreEntityChanges
                        = (session, entity, id) => entity is User u && u.IgnoreChanges;
                }
            });
            using (var s = store.OpenSession())
            {
                s.Store(new User{Name = "Oren"}, "users/oren");
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                User user = s.Load<User>("users/oren");
                user.Name = "Arava";
                user.IgnoreChanges = true;
                s.SaveChanges();
            }

            using (var s = store.OpenSession())
            {
                User user = s.Load<User>("users/oren");
                Assert.Equal("Oren", user.Name);
            }
        }
    }
}
