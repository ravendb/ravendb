using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Indexes.Configuration
{
    public class FaultyInMemoryIndexConfiguration : IndexingConfiguration
    {
        private readonly PathSetting _storagePath;

        public FaultyInMemoryIndexConfiguration(PathSetting storagePath, RavenConfiguration databaseConfiguration)
            : base(databaseConfiguration)
        {
            _storagePath = storagePath;
        }

        public override bool RunInMemory => false;

        public override PathSetting StoragePath => _storagePath; 
    }
}