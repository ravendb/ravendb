using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Identity;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8959 : RavenTestBase
    {
        public RavenDB_8959(ITestOutputHelper output) : base(output)
        {
        }

        public class SingleNodeAsyncMultiDatabaseHiLoIdGenerator : AsyncMultiDatabaseHiLoIdGenerator
        {
            public SingleNodeAsyncMultiDatabaseHiLoIdGenerator(DocumentStore store) : base(store)
            {
            }

            public override AsyncMultiTypeHiLoIdGenerator GenerateAsyncMultiTypeHiLoFunc(string dbName)
            {
                return new SingleNodeAsyncMultiTypeHiLoIdGenerator(Store, dbName);
            }

            public class SingleNodeAsyncMultiTypeHiLoIdGenerator : AsyncMultiTypeHiLoIdGenerator
            {
                public SingleNodeAsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName) : base(store, dbName)
                {
                }

                protected override AsyncHiLoIdGenerator CreateGeneratorFor(string tag)
                {
                    return new SingleNodeAsyncHiLoIdGenerator(tag, Store, DbName, Conventions.IdentityPartsSeparator);
                }

                public class SingleNodeAsyncHiLoIdGenerator : AsyncHiLoIdGenerator
                {
                    public SingleNodeAsyncHiLoIdGenerator(string tag, DocumentStore store, string dbName, char identityPartsSeparator) : base(tag, store, dbName,
                        identityPartsSeparator)
                    {
                    }

                    protected override string GetDocumentIdFromId(long nextId)
                    {
                        return $"{Prefix}{nextId}";
                    }
                }
            }
        }

        [Fact]
        public void CanCustomizeHiloGeneration()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    var gen = new SingleNodeAsyncMultiDatabaseHiLoIdGenerator(documentStore);
                    documentStore.Conventions.AsyncDocumentIdGenerator = gen.GenerateDocumentIdAsync;
                }
            }))
            {
                using (var s = store.OpenSession())
                {
                    var entity = new User();
                    s.Store(entity);
                    Assert.Equal("users/1", entity.Id);
                }
            }
        }
    }
}
