using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Connection;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Core.Indexing
{
    public class IndexFieldsValidation : RavenTestBase
    {
        private class InvalidMultiMapIndex : AbstractMultiMapIndexCreationTask
        {
            public InvalidMultiMapIndex()
            {
                AddMap<User>(users => from u in users select new { u.Name });
                AddMap<Company>(companies => from c in companies select new { c.Email });
            }
        }

        private class InvalidMapReduceIndex : AbstractIndexCreationTask<User, InvalidMapReduceIndex.Result>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public InvalidMapReduceIndex()
            {
                Map = users => from u in users select new { u.Name };
                Reduce = results => from r in results group r by r.Name into g select new { Email = g.Key };
            }
        }

        [Fact]
        public async Task ShouldThrow()
        {
            using (var store = await GetDocumentStore())
            {
                var e = Assert.Throws<ErrorResponseException>(() => new InvalidMultiMapIndex().Execute(store));
                Assert.Contains("Map and Reduce functions of a index must return identical types.", e.Message);

                e = Assert.Throws<ErrorResponseException>(() => new InvalidMapReduceIndex().Execute(store));
                Assert.Contains("Map and Reduce functions of a index must return identical types.", e.Message);
            }
        }
    }
}