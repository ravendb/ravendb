using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2862 : RavenTestBase
    {
        private class FullEntity
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Addresss { get; set; }
        }

        private class MinifiedEntity
        {
            public string Name { get; set; }
        }

        private class EntityTransformer : AbstractTransformerCreationTask<FullEntity>
        {
            public EntityTransformer()
            {
                TransformResults = results => from result in results
                                              select new MinifiedEntity
                                              {
                                                  Name = result.Name
                                              };
            }
        }

        [Fact]
        public async Task AsyncLoadWithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                new EntityTransformer().Execute(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new FullEntity()
                    {
                        Name = "John",
                        Id = "FullEntities/John",
                        Addresss = "3764 Elvis Presley Boulevard",
                        Age = 66
                    }, "FullEntities/John");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.LoadAsync<EntityTransformer, MinifiedEntity>(string.Format("{0}/{1}", "FullEntities", "John"));
                    Assert.NotNull(result);
                }
            }
        }
    }
}
