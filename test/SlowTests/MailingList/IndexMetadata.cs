using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.MailingList
{
    public class IndexMetadata : RavenTestBase
    {
        public IndexMetadata(ITestOutputHelper output) : base(output)
        {
        }

        private class Users_DeleteStatus : AbstractMultiMapIndexCreationTask
        {
            public Users_DeleteStatus()
            {
                AddMap<User>(users => from user in users
                                      select new
                                      {
                                          Deleted = MetadataFor(user)["Deleted"]
                                      });
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void WillGenerateProperIndex()
        {
            var usersDeleteStatus = new Users_DeleteStatus { Conventions = new DocumentConventions() };
            var indexDefinition = usersDeleteStatus.CreateIndexDefinition();
            Assert.Contains("Deleted = this.MetadataFor(user)[\"Deleted\"]", indexDefinition.Maps.First());
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void CanCreateIndex()
        {
            using (var store = GetDocumentStore())
            {
                new Users_DeleteStatus().Execute(store);
            }
        }
    }
}
