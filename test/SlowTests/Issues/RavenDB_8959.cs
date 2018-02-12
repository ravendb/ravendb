using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8959 : RavenTestBase
    {
        public class SingleNodeAsyncMultiDatabaseHiLoIdGenerator : AsyncMultiDatabaseHiLoIdGenerator
        {
            public SingleNodeAsyncMultiDatabaseHiLoIdGenerator(DocumentStore store, DocumentConventions conventions) : base(store, conventions)
            {
            }

            public override AsyncMultiTypeHiLoIdGenerator GenerateAsyncMultiTypeHiLoFunc(string dbName)
            {
                return new SingleNodeAsyncMultiTypeHiLoIdGenerator(Store, dbName, Conventions);
            }

            public class SingleNodeAsyncMultiTypeHiLoIdGenerator : AsyncMultiTypeHiLoIdGenerator
            {
                public SingleNodeAsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName, DocumentConventions conventions) : base(store, dbName, conventions)
                {
                }

                protected override AsyncHiLoIdGenerator CreateGeneratorFor(string tag)
                {
                    return new SingleNodeAsyncHiLoIdGenerator(tag, Store, DbName, Conventions.IdentityPartsSeparator);
                }

                public class SingleNodeAsyncHiLoIdGenerator : AsyncHiLoIdGenerator
                {
                    public SingleNodeAsyncHiLoIdGenerator(string tag, DocumentStore store, string dbName, string identityPartsSeparator) : base(tag, store, dbName,
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
                    var gen = new SingleNodeAsyncMultiDatabaseHiLoIdGenerator(documentStore, documentStore.Conventions);
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
