using System.Reflection;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Config.Settings;

namespace Raven.Server.Documents.Indexes.Configuration
{
    public class SingleIndexConfiguration : IndexingConfiguration
    {
        private readonly RavenConfiguration _databaseConfiguration;

        public SingleIndexConfiguration(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration)
            : base(databaseConfiguration)
        {
            _databaseConfiguration = databaseConfiguration;

            Initialize(
                key =>
                    new SettingValue(clientConfiguration.GetValue(key) ?? databaseConfiguration.GetSetting(key),
                        databaseConfiguration.GetServerWideSetting(key)),
                databaseConfiguration.GetServerWideSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory)),
                databaseConfiguration.ResourceType, 
                databaseConfiguration.ResourceName, 
                throwIfThereIsNoSetMethod: false);
        }

        public override bool Disabled => _databaseConfiguration.Indexing.Disabled;

        public override bool RunInMemory => _databaseConfiguration.Indexing.RunInMemory;

        public override PathSetting TempPath => _databaseConfiguration.Indexing.TempPath ?? _databaseConfiguration.Indexing.StoragePath;

        public IndexUpdateType CalculateUpdateType(SingleIndexConfiguration newConfiguration)
        {
            var result = IndexUpdateType.None;
            foreach (var property in GetConfigurationProperties())
            {
                var currentValue = property.GetValue(this);
                var newValue = property.GetValue(newConfiguration);

                if (Equals(currentValue, newValue))
                    continue;

                var updateTypeAttribute = property.GetCustomAttribute<IndexUpdateTypeAttribute>();

                if (updateTypeAttribute.UpdateType == IndexUpdateType.Reset)
                    return IndexUpdateType.Reset; // worst case, we do not need to check further

                result = updateTypeAttribute.UpdateType;
            }

            return result;
        }
    }
}
