using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
    public class KeyLength : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void DifferentKeysWithTheSameFirst127CharactersAreDifferent(string storage)
        {
            var identicalPrefix = new string('x', 127);
            var aId = identicalPrefix + "a";
            var bId = identicalPrefix + "b";
            using (var s = NewDocumentStore(requestedStorage: storage))
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