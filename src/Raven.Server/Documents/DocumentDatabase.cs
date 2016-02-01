using Raven.Server.Config;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        public DocumentDatabase(RavenConfiguration configuration)
        {
            Configuration = configuration;
        }

        public void Dispose()
        {
            
        }

        public string Name { get; }

        public string ResourceName { get; }

        public RavenConfiguration Configuration { get; }

        public DocumentActions Documents { get; set; }
    }
}