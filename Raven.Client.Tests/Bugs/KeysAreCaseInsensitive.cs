using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class KeysAreCaseInsensitive : LocalClientTest
    {
        [Fact]
        public void WillNotGoToTheServerForLoadingDocumentWithSameIdDifferentCase()
        {
            using(var s = NewDocumentStore())
            {
                using(var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "Ayende",
                        Email = "Ayende@ayende.com",
                        Name = "Ayende Rahien"
                    });

                    session.SaveChanges();
                }

                using (var session = s.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("Ayende"));
                    Assert.NotNull(session.Load<User>("AYENDE"));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanIndexIdWithDifferntCasing()
        {
            using (var s = NewDocumentStore())
            {
                using (var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "Ayende",
                        Email = "Ayende@ayende.com",
                        Name = "Ayende Rahien"
                    });

                    session.SaveChanges();
                }


                using (var session = s.OpenSession())
                {
                    session.Advanced.LuceneQuery<User>().WaitForNonStaleResults().FirstOrDefault();
                }


                using (var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "AYENDE",
                        Email = "Ayende@ayende.com",
                        Name = "Ayende Rahien"
                    });

                    session.SaveChanges();
                }

                using (var session = s.OpenSession())
                {
                    var count = session.Advanced.LuceneQuery<User>().WaitForNonStaleResults().Count();
                    Assert.Equal(1, count);
                }
            }
        }

        [Fact]
        public void CanLoadIdWithDifferentCase()
        {
            using (var s = NewDocumentStore())
            {
                using (var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "Ayende",
                        Email = "Ayende@ayende.com",
                        Name = "Ayende Rahien"
                    });

                    session.SaveChanges();
                }

                using (var session = s.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("AYENDE"));
                }
            }
        }
    }
}