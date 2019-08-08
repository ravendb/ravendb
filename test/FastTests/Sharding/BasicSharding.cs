using Xunit;

namespace FastTests.Sharding
{
    public class BasicSharding : ShardedTestBase
    {
        public class User
        {

        }

        [Fact]
        public void CanCreateShardedDatabase()
        {
            using (var store = GetShardedDocumentStore())
            {
                WaitForUserToContinueTheTest(store);
                using (var s = store.OpenSession())
                {
                    var u = s.Load<User>("users/1");
                    Assert.Null(u);
                }
            }
        }
    }
}
