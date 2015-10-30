using System.IO;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class CompilingIndexesCreatesCacheDirectory : RavenTest
    {
        private readonly string compiledIndexCacheDirectory;

        public CompilingIndexesCreatesCacheDirectory()
        {
            compiledIndexCacheDirectory = Path.Combine(Path.GetTempPath(), "TempIndexCacheDirectory");
        }

        [Fact]
        public void CanDealWithMissingCacheDirectory()
        {
            using (var store = NewDocumentStore())
            {
                store.Configuration.CompiledIndexCacheDirectory = compiledIndexCacheDirectory;

                DeleteDirectory(store.Configuration.CompiledIndexCacheDirectory);

                var index = new Index();

                Assert.DoesNotThrow(() => index.Execute(store));
                Assert.True(Directory.Exists(compiledIndexCacheDirectory));
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            DeleteDirectory(compiledIndexCacheDirectory);
        }

        private static void DeleteDirectory(string directoryPath)
        {
            if (Directory.Exists(directoryPath))
            {
                Directory.Delete(directoryPath, true);
            }
        }

        class Entity
        {
            public string Id { get; set; }
        }

        class Index : AbstractIndexCreationTask<Entity>
        {
            public Index()
            {
                Map = entities => from entity in entities
                                  select new
                                  {
                                      entity.Id
                                  };
            }
        }
    }
}
