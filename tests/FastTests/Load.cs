using System.Collections.Generic;
using Raven.Tests.Core.Utils.Entities;

using Xunit;

namespace NewClientTests.NewClient
{
    public class Load :  RavenTestBase
    {
        [Fact]
        public void Load_Document_By_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Document_By_ValueType_id()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<User>(2);
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "Hibernating Rhinos");
                }
            }
        }

        [Fact]
        public void Load_Documents_By_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var users = newSession.Load<User>(1, 2);
                    Assert.Equal(users.Length, 2);
                }
            }
        }

        [Fact]
        public void Load_Documents_By_IEnumerable_ValueType_ids()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "RavenDB" }, "users/1");
                    session.Store(new User { Name = "Hibernating Rhinos" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var users = newSession.Load<User>(new List<System.ValueType> { 1, 2 });
                    Assert.Equal(users.Length, 2);

                }
            }
        }

        [Fact]
        public void Load_Null_Should_Return_Null()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Tony Montana" }, "users/1");
                    session.Store(new User { Name = "Tony Soprano" }, "users/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var user1 = newSession.Load<User>((string) null);
                    Assert.Null(user1);

                    var orderedArrayOfIdsWithNull = new[] {"users/1", null, "users/2", null};
                    var users1 = newSession.Load<User>(orderedArrayOfIdsWithNull);
                    Assert.Equal(users1[0].Id, "users/1");
                    Assert.Null(users1[1]);
                    Assert.Equal(users1[2].Id, "users/2");
                    Assert.Null(users1[3]);
                    
                    var unorderedSetOfIdsWithNull = new HashSet<string>() { "users/1", null, "users/2", null };
                    var users2 = newSession.Load<User>(unorderedSetOfIdsWithNull);
                    Assert.Equal(users2.Length, 3);
                    Assert.True(users2[0]==null || users2[1] == null || users2[2] == null);
                }
            }
        }

        [Fact]
        public void Load_Document_With_Int_Array_And_Long_Array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new GeekPerson { Name = "Bebop", FavoritePrimes = new[] { 13, 43, 443, 997 }, FavoriteVeryLargePrimes = new[] { 5000000029, 5000000039 } }, "geeks/1");
                    session.Store(new GeekPerson { Name = "Rocksteady", FavoritePrimes = new[] { 2, 3, 5, 7 }, FavoriteVeryLargePrimes = new[] { 999999999989 } }, "geeks/2");
                    session.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var geek1 = newSession.Load<GeekPerson>("geeks/1");
                    var geek2 = newSession.Load<GeekPerson>("geeks/2");

                    Assert.Equal(geek1.FavoritePrimes[1], 43);
                    Assert.Equal(geek1.FavoriteVeryLargePrimes[1], 5000000039);

                    Assert.Equal(geek2.FavoritePrimes[3], 7);
                    Assert.Equal(geek2.FavoriteVeryLargePrimes[0], 999999999989);
                }
            }
        }
    }
}
