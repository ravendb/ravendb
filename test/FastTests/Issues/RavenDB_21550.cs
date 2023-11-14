using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_21550 : RavenTestBase
    {
        public RavenDB_21550(ITestOutputHelper output)
            : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Should_Handle_ToArray()
        {
            using (var store = GetDocumentStore())
            {
                await new Index().ExecuteAsync(store);

                const string name = "Grisha";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = name });
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<User, Index>()
                        .ProjectInto<Index.Result>()
                        .FirstOrDefaultAsync();

                    Assert.NotNull(result);
                    Assert.Equal(name.ToArray(), result.Array);
                }
            }
        }

        public class Index : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public char[] Array { get; set; }
            }

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from user in docs.Users
select new { Array = user.Name.ToArray() }" },
                    Fields =
                    {
                        {
                            nameof(Result.Array), new IndexFieldOptions()
                            {
                                Storage = FieldStorage.Yes
                            }
                        }
                    }
                };
            }
        }
    }
}
