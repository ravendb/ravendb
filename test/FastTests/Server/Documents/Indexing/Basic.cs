using System;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Tests.Core;

using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class Basic : RavenTestBase
    {
        [Fact]
        public void CheckDispose()
        {
            using (var storage = CreateDocumentsStorage())
            {
                var indexingConfiguration = new IndexingConfiguration(() => true, () => null);

                var index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String, false) }), storage, indexingConfiguration);
                index.Dispose();

                Assert.Throws<ObjectDisposedException>(() => index.Dispose());
                Assert.Throws<ObjectDisposedException>(() => index.Execute(CancellationToken.None));
                Assert.Throws<ObjectDisposedException>(() => index.Query(new IndexQuery()));

                index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String, false) }), storage, indexingConfiguration);
                index.Execute(CancellationToken.None);
                index.Dispose();

                using (var cts = new CancellationTokenSource())
                {
                    index = AutoIndex.CreateNew(1, new AutoIndexDefinition("Users", new[] { new AutoIndexField("Name", SortOptions.String, false) }), storage, indexingConfiguration);
                    index.Execute(cts.Token);

                    cts.Cancel();

                    index.Dispose();
                }
            }
        }

        private static DocumentsStorage CreateDocumentsStorage()
        {
            var storage = new DocumentsStorage("TestStorage", new RavenConfiguration { Core = { RunInMemory = true } });
            storage.Initialize();

            return storage;
        }
    }
}