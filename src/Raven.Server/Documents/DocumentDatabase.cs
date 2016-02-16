using System.Threading;

using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Tasks;
using Raven.Server.ServerWide;

using Voron;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private readonly CancellationTokenSource CancellationTokenSource = new CancellationTokenSource();

        public DocumentDatabase(string name, RavenConfiguration configuration)
        {
            Name = name;
            Configuration = configuration;
            CancellationToken = CancellationTokenSource.Token;

            Notifications = new DatabaseNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            TasksStorage = new TasksStorage();
        }

        public string Name { get; }

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }

        public CancellationToken CancellationToken { get; }

        public DocumentsStorage DocumentsStorage { get; }

        public DatabaseNotifications Notifications { get; }

        public IndexStore IndexStore { get; }

        public TasksStorage TasksStorage { get; }

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
            DocumentsStorage.Dispose();
        }
    }
}