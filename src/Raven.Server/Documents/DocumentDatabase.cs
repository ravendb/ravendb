using Raven.Server.Config;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        public DocumentDatabase(string name, RavenConfiguration configuration)
        {
            Name = name;
            Configuration = configuration;
            DocumentsStorage = new DocumentsStorage("test", configuration);
            DocumentsStorage.Initialize();
        }

        public DocumentsStorage DocumentsStorage { get; }

        public void Dispose()
        {
            
        }

        public string Name { get; }

        public string ResourceName { get; }

        public RavenConfiguration Configuration { get; }

        public DocumentActions Documents { get; set; }
    }
}