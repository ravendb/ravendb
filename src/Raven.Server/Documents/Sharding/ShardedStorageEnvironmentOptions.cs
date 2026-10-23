using Raven.Client.ServerWide.Sharding;
using Sparrow.Server;
using Voron;
using Voron.Exceptions;
using Voron.Util.Settings;

namespace Raven.Server.Documents.Sharding
{
    // The bucket index key generators are static methods, resolved by reflection from the schema, so the only
    // thing they are handed is the transaction. These options are how the sharding configuration reaches them.
    public sealed class ShardedDirectoryStorageEnvironmentOptions : StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly ShardingConfiguration _configuration;

        public ShardedDirectoryStorageEnvironmentOptions(VoronPathSetting basePath, VoronPathSetting tempPath, VoronPathSetting journalPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            ShardedDocumentDatabase database, ShardingConfiguration configuration)
            : base(basePath, tempPath, journalPath, ioChangesNotifications, catastrophicFailureNotification)
        {
            _database = database;
            _configuration = configuration;
        }

        public ShardingConfiguration ShardingConfiguration => _database?.ShardingConfiguration ?? _configuration;

        public override bool CanRebuildDynamicKeyIndexes => true;
    }

    public sealed class ShardedPureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly ShardingConfiguration _configuration;

        public ShardedPureMemoryStorageEnvironmentOptions(string name, VoronPathSetting tempPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            ShardedDocumentDatabase database, ShardingConfiguration configuration)
            : base(name, tempPath, ioChangesNotifications, catastrophicFailureNotification)
        {
            _database = database;
            _configuration = configuration;
        }

        public ShardingConfiguration ShardingConfiguration => _database?.ShardingConfiguration ?? _configuration;

        public override bool CanRebuildDynamicKeyIndexes => true;
    }
}
