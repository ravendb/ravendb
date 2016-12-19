using System.Collections.Specialized;
using System.IO;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;
using Raven.Database.Config;
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    public class RavenDB_3530 : RavenTest
    {
        private readonly string indexCachePath;
        private bool useConfiguration;

        public RavenDB_3530()
        {
            indexCachePath = Path.GetFullPath(NewDataPath("CompiledIndexCache"));
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            if (useConfiguration)
            {
                configuration.CompiledIndexCacheDirectory = indexCachePath;
            }
            else
            {
                configuration.Settings = new NameValueCollection
                {
                    { "Raven/CompiledIndexCacheDirectory", indexCachePath }
                };
                configuration.Initialize();
            }
        }

        [Fact]
        public void SettingCompileIndexCacheDirectoryWithSettingsNVC()
        {
            useConfiguration = false;

            using (var embeddedStore = NewDocumentStore(runInMemory: false))
            {
                embeddedStore.ExecuteIndex(new Index());
                var directoryOfCompiledIndexExsits = Directory.Exists(indexCachePath);
                Assert.True(directoryOfCompiledIndexExsits, "Unable to find directory: " + indexCachePath);
            }
        }

        [Fact]
        public void SettingCompileIndexCacheDirectoryUsingConfiguration()
        {
            useConfiguration = true;

            using (var embeddedStore = NewDocumentStore(runInMemory: false))
            {
                embeddedStore.ExecuteIndex(new Index());
                var directoryOfCompiledIndexExsits = Directory.Exists(indexCachePath);
                Assert.True(directoryOfCompiledIndexExsits, "Unable to find directory: " + indexCachePath);
            }
        }

        public class Person
        {
            public string Name;
            public int Age;
        }

        public class Index : AbstractIndexCreationTask<Person>
        {
            public Index()
            {
                Map = persons =>
                    from person in persons
                    select new {person.Name, person.Age};
            }
        }
    }
}
