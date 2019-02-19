using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class KeyLength : RavenTestBase
    {
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void DifferentKeysWithTheSameFirst127CharactersAreDifferent()
        {
            var identicalPrefix = new string('x', 127);
            var aId = identicalPrefix + "a";
            var bId = identicalPrefix + "b";
            using (var s = GetDocumentStore())
            {
                using (var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = aId,
                        Name = "a"
                    });

                    session.SaveChanges();
                }
                using (var session = s.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = bId,
                        Name = "b"
                    });

                    session.SaveChanges();
                }
            }
        }
    }
}
