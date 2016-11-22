using Raven.Client.Indexing;
using Raven.Server.Config;
using Raven.Server.Config.Categories;

namespace Raven.Server.Documents.Indexes
{
    public class IndexingConfigurationWithClientOverrides : IndexingConfiguration
    {
        private readonly IndexConfiguration _clientConfiguration;
        private readonly RavenConfiguration _databaseConfiguration;

        public IndexingConfigurationWithClientOverrides(RavenConfiguration databaseConfiguration)
            : this(null, databaseConfiguration)
        {
        }

        public IndexingConfigurationWithClientOverrides(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration)
            : base(null, null)
        {
            _clientConfiguration = clientConfiguration;
            _databaseConfiguration = databaseConfiguration;

            Initialize(key => clientConfiguration.GetValue(key) ?? databaseConfiguration.GetSetting(key), throwIfThereIsNoSetMethod: false);
        }

        public override bool RunInMemory => _databaseConfiguration.Indexing.RunInMemory;
        public override bool Disabled => _databaseConfiguration.Indexing.Disabled;
        public override string IndexStoragePath => _databaseConfiguration.Indexing.IndexStoragePath;
    }
}