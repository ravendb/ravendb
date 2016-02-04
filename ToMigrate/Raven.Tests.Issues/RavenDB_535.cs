using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_535 : RavenTest
    {
        [Fact]
        public void CheapGetNextIdentityValueWithoutOverwritingOnExistingDocuments()
        {
            using(var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 1337; i++)
                    {
                        session.Store(new User());
                    }
                    session.SaveChanges();
                }
                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    int tries;
                    var val = store.SystemDatabase.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("users/", accessor,
                                                                                                               out tries);
                    Assert.True(30 > tries);
                    Assert.Equal(1338, val);
                });

                store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                {
                    int tries;
                    var val = store.SystemDatabase.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("users/", accessor,
                                                                                                               out tries);
                    Assert.Equal(1, tries);
                    Assert.Equal(1339, val);
                });
            }
        }

        public class User{}
    }
}
