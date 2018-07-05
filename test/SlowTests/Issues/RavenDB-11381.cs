using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11381 : RavenTestBase
    {
        private class User
        {
#pragma warning disable 649
            public string Name;
#pragma warning restore 649
        }

        private static bool IsValid(User u) => true;

        private class MyIndex : AbstractIndexCreationTask<User>
        {
            public MyIndex()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };
            }
        }

        [Fact]
        public void CanSpecifyCustomIndexName()
        {
            var conventions = new DocumentConventions
            {
                TypeIsKnownServerSide = t => t == typeof(RavenDB_11381)
            };
            var map = new MyIndex
            {
                Conventions = conventions
            }.CreateIndexDefinition().Maps.First();
            Assert.Contains("RavenDB_11381.IsValid", map);
        }
    }
}
