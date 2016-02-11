using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Indexes;

namespace Raven.Server
{
    public class DocumentDatabase : IResourceStore
    {
        public DocumentDatabase(string name, RavenConfiguration configuration)
        {
            Name = name;
            Configuration = configuration;

            DocumentStorage = new DocumentsStorage(name, configuration);
            IndexStore = new IndexStore(DocumentStorage);
        }

        public string Name { get; }

        public string ResourceName => $"db/{Name}";

        public RavenConfiguration Configuration { get; }

        public DocumentsStorage DocumentStorage { get; }

        public IndexStore IndexStore { get; }

        public void Initialize()
        {
            DocumentStorage.Initialize();
            IndexStore.Initialize();
        }

        public void Dispose()
        {
            DocumentStorage.Dispose();
        }
    }
}