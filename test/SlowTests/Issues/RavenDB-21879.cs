using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21879 : RavenTestBase
{
    public RavenDB_21879(ITestOutputHelper output) : base(output)
    {
    }

    private class SingleNodeAsyncMultiDatabaseHiLoIdGenerator : AsyncMultiDatabaseHiLoIdGenerator
    {
        public SingleNodeAsyncMultiDatabaseHiLoIdGenerator(DocumentStore store) : base(store)
        {
        }

        public override AsyncMultiTypeHiLoIdGenerator GenerateAsyncMultiTypeHiLoFunc(string dbName)
        {
            return new SingleNodeAsyncMultiTypeHiLoIdGenerator(Store, dbName);
        }

        private class SingleNodeAsyncMultiTypeHiLoIdGenerator : AsyncMultiTypeHiLoIdGenerator
        {
            public SingleNodeAsyncMultiTypeHiLoIdGenerator(DocumentStore store, string dbName) : base(store, dbName)
            {
            }

            protected override AsyncHiLoIdGenerator CreateGeneratorFor(string tag)
            {
                return new SingleNodeAsyncHiLoIdGenerator(tag, Store, DbName, Conventions.IdentityPartsSeparator);
            }

            private class SingleNodeAsyncHiLoIdGenerator : AsyncHiLoIdGenerator
            {
                public SingleNodeAsyncHiLoIdGenerator(string tag, DocumentStore store, string dbName, char identityPartsSeparator) : base(tag, store, dbName,
                    identityPartsSeparator)
                {
                }
                
                protected override string GetDocumentIdFromId(NextId nextId)
                {
                    return $"{Prefix}{nextId.Id}";
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task CustomHiloIdGeneration()
    {
        SingleNodeAsyncMultiDatabaseHiLoIdGenerator customHiloIdGenerator = null;
        
        var options = new Options()
        {
            ModifyDocumentStore = documentStore =>
            {
                customHiloIdGenerator = new SingleNodeAsyncMultiDatabaseHiLoIdGenerator(documentStore);

                documentStore.Conventions.AsyncDocumentIdGenerator = customHiloIdGenerator.GenerateDocumentIdAsync;
            }
        };
        
        using (var store = GetDocumentStore(options))
        { 
            var idForDto = await customHiloIdGenerator.GenerateNextIdForAsync(null, typeof(Dto));

            Assert.Equal(1, idForDto);
            
            var dto = new Dto() { Name = "CoolName" };

            var fullIdForDto = await customHiloIdGenerator.GenerateDocumentIdAsync(null, dto);
            
            Assert.Equal("dtos/2", fullIdForDto);
        }
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task UsingDefaultHiLoGeneratorWithExistingCustomOneShouldThrow()
    {
        SingleNodeAsyncMultiDatabaseHiLoIdGenerator customHiloIdGenerator = null;

        var options = new Options()
        {
            ModifyDocumentStore = documentStore =>
            {
                customHiloIdGenerator = new SingleNodeAsyncMultiDatabaseHiLoIdGenerator(documentStore);

                documentStore.Conventions.AsyncDocumentIdGenerator = customHiloIdGenerator.GenerateDocumentIdAsync;
            }
        };

        using (var store = GetDocumentStore(options))
        {
            var e1 = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var nextId = await store.HiLoIdGenerator.GenerateNextIdForAsync(null, typeof(Dto));
            });

            Assert.Contains($"Overwriting {nameof(DocumentConventions.AsyncDocumentIdGenerator)} convention does not allow usage of default HiLo generator, you should use your own one.", e1.Message);
            
            var e2 = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                var dto = new Dto() { Name = "CoolName" };
                var nextFullId = await store.HiLoIdGenerator.GenerateDocumentIdAsync(null, dto);
            });
            
            Assert.Contains($"Overwriting {nameof(DocumentConventions.AsyncDocumentIdGenerator)} convention does not allow usage of default HiLo generator, you should use your own one.", e2.Message);
            
            var dto = new Dto() { Name = "CoolName" };
            
            using (var session = store.OpenSession())
            {
                session.Store(dto);
                session.SaveChanges();
            }
            
            Assert.Equal("dtos/1", dto.Id);
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}
