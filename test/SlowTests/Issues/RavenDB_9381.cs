using FastTests.Voron;
using Raven.Client.Documents.Indexes;
using Raven.Server.Indexing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9381 : StorageTest
    {
        public RavenDB_9381(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Lucene_directory_must_be_aware_of_created_outputs()
        {
            using (var tx = Env.WriteTransaction())
            using (var cache = new TempFileCache(Env.Options))
            {
                var dir = new LuceneVoronDirectory(null, tx, Env, cache, LuceneIndexInputType.Standard);

                var state = new VoronState(tx);

                dir.CreateOutput("file", state);
                Assert.True(dir.FileExists("file", state));

                dir.DeleteFile("file", state);
                Assert.False(dir.FileExists("file", state));
            }
        }
    }
}
