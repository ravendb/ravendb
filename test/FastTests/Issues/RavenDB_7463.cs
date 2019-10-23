using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Database;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7463 : RavenTestBase
    {
        public RavenDB_7463(ITestOutputHelper output) : base(output)
        {
        }

        public class SimpleIndex : AbstractIndexCreationTask<User>
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from doc in docs select new { doc.Name};" },
                };
            }
        }

        [Fact]
        public void ShouldThrowDatabaseDoesNotExistsException()
        {
            var databaseName = "RavenDB_7463" + Guid.NewGuid();            

            using (var store = new DocumentStore
            {
                Urls = new[]{ Server.WebUrl },
                Database = databaseName,
            })
            {
                store.Initialize();
                Assert.Throws<DatabaseDoesNotExistException>(() => new SimpleIndex().Execute(store));
            }
        }
    }
}
