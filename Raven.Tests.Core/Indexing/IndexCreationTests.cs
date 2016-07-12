// -----------------------------------------------------------------------
//  <copyright file="IndexCreationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Core.Indexing
{
    public class IndexCreationTests : RavenCoreTestBase
    {
#if DNXCORE50
        public IndexCreationTests(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public async Task IndexCreationShouldWorkAsync()
        {
            using (var store = GetDocumentStore())
            {
                await IndexCreation.CreateIndexesAsync(typeof(Companies_SortByName).Assembly(), store);

                var indexes = store.DatabaseCommands.GetIndexes(0, 128);
                var transformers = store.DatabaseCommands.GetTransformers(0, 128);

                Assert.True(indexes.Length > 0);
                Assert.True(transformers.Length > 0);
            }
        }

        [Fact]
        public void IndexCreationShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                IndexCreation.CreateIndexes(typeof(Companies_SortByName).Assembly(), store);

                var indexes = store.DatabaseCommands.GetIndexes(0, 128);
                var transformers = store.DatabaseCommands.GetTransformers(0, 128);

                Assert.True(indexes.Length > 0);
                Assert.True(transformers.Length > 0);
            }
        }
    }
}