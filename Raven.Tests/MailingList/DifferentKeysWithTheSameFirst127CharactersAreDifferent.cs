using System;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class KeyLength : RavenTest
    {
        [Fact]
        public void DifferentKeysWithTheSameFirst127CharactersAreDifferent()
        {
            var identicalPrefix = new string('x', 127);
            var aId = identicalPrefix + "a";
            var bId = identicalPrefix + "b";
            using (var s = NewDocumentStore(requestedStorage: "esent"))
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

                    Assert.Throws<NotSupportedException>(() => session.SaveChanges());
                }
            }
        }
    }
}