using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
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
                    session.Store(new Common.Dto.User
                    {
                        Id = aId,
                        Name = "a"
                    });

                    session.SaveChanges();
                }
                using (var session = s.OpenSession())
                {
                    session.Store(new Common.Dto.User
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