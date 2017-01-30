using Raven.Server.Config;
using Raven.Server.Config.Categories;

namespace Raven.Server.Documents.Indexes.Configuration
{
    public class FaultyInMemoryIndexConfiguration : IndexingConfiguration
    {
        private readonly string _storagePath;

        public FaultyInMemoryIndexConfiguration(string storagePath, RavenConfiguration databaseConfiguration)
            : base(() => databaseConfiguration.DatabaseName, null, null, databaseConfiguration.PerformanceHints.MaxWarnIndexOutputsPerDocument)
        {
            _storagePath = storagePath;
        }

        public override bool RunInMemory => false;

        public override string StoragePath => _storagePath; 
    }
}