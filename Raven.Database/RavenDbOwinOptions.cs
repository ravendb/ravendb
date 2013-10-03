using Microsoft.Owin;
using Owin;
using Raven.Database.Config;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database
{
    public class RavenDbOwinOptions
    {
        private readonly DatabasesLandlord databasesLandlord;
        private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;

        public MixedModeRequestAuthorizer MixedModeRequestAuthorizer
        {
            get { return mixedModeRequestAuthorizer; }
        }

        public DatabasesLandlord Landlord
        {
            get { return databasesLandlord; }
        }

        internal IAppBuilder Branch { get; set; }

        public RavenDbOwinOptions(InMemoryRavenConfiguration configuration, DocumentDatabase documentDatabase)
        {
            databasesLandlord = new DatabasesLandlord(documentDatabase);
            mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
            mixedModeRequestAuthorizer.Initialize(documentDatabase, new RavenServer(databasesLandlord.SystemDatabase, configuration));
        }

        private class RavenServer : IRavenServer
        {
            private readonly DocumentDatabase systemDatabase;
            private readonly InMemoryRavenConfiguration systemConfiguration;

            public RavenServer(DocumentDatabase systemDatabase, InMemoryRavenConfiguration systemConfiguration)
            {
                this.systemDatabase = systemDatabase;
                this.systemConfiguration = systemConfiguration;
            }

            public DocumentDatabase SystemDatabase { get { return systemDatabase; } }
            public InMemoryRavenConfiguration SystemConfiguration { get { return systemConfiguration; } }
        }
    }
}