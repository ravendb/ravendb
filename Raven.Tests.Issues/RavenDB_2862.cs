using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Permissions;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2862 : RavenTest
    {
        public class FullEntity
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Addresss { get; set; }
        }

        public class MinifiedEntity
        {
            public string Name { get; set; }
        }

        public class EntityTransformer : AbstractTransformerCreationTask<FullEntity>
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
            using (var store = NewDocumentStore())
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
                    var result = await session.LoadAsync<EntityTransformer, MinifiedEntity>(string.Format("{0}/{1}", "FullEntities","John"));
                    Assert.NotNull(result);
                }


            }
        }
    }
}
