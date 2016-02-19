using System.Threading;

using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;

using Voron;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();

        public DocumentDatabase(string name, RavenConfiguration configuration)
        {
            Name = name;
            Configuration = configuration;

            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
        }

        public string Name { get; }

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; }

        public DocumentsNotifications Notifications { get; }

        public IndexStore IndexStore { get; }

        public void Initialize()
        {
            DocumentsStorage.Initialize();
            IndexStore.Initialize();
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            DocumentsStorage.Initialize(options);
            IndexStore.Initialize();
        }

        public void Dispose()
        {
            _databaseShutdown.Cancel();

            DocumentsStorage?.Dispose();
            IndexStore?.Dispose();
        }
    }
}