using Raven.Client.ServerWide.Sharding;
using Sparrow.Server;
using Voron;
using Sparrow.Server.Logging;
using Voron.Exceptions;

namespace Raven.Server.Documents.Sharding
{
    public interface IShardedStorageEnvironmentOptions
    {
        ShardingConfiguration ShardingConfiguration { get; }
    }

    public sealed class ShardedDirectoryStorageEnvironmentOptions : StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions, IShardedStorageEnvironmentOptions
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly ShardingConfiguration _configuration;

        public ShardedDirectoryStorageEnvironmentOptions(string path, string tempPath, string journalPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            LoggingResource loggingResource, ShardedDocumentDatabase database)
            : base(path, tempPath, journalPath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent: null)
        {
            _database = database;
        }

        public ShardedDirectoryStorageEnvironmentOptions(string path, string tempPath, string journalPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            LoggingResource loggingResource, ShardingConfiguration configuration)
            : base(path, tempPath, journalPath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent: null)
        {
            _configuration = configuration;
        }

        public ShardingConfiguration ShardingConfiguration => _database?.ShardingConfiguration ?? _configuration;
    }

    public sealed class ShardedPureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions, IShardedStorageEnvironmentOptions
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly ShardingConfiguration _configuration;

        public ShardedPureMemoryStorageEnvironmentOptions(string name, string tempPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            LoggingResource loggingResource, ShardedDocumentDatabase database)
            : base(name, tempPath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent: null)
        {
            _database = database;
        }

        public ShardedPureMemoryStorageEnvironmentOptions(string name, string tempPath,
            IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification,
            LoggingResource loggingResource, ShardingConfiguration configuration)
            : base(name, tempPath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent: null)
        {
            _configuration = configuration;
        }

        public ShardingConfiguration ShardingConfiguration => _database?.ShardingConfiguration ?? _configuration;
    }
}
