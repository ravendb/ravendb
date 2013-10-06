using Raven.Database.Config;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database
{
    public class RavenDBOptions
    {
        private readonly DatabasesLandlord databasesLandlord;
        private readonly DocumentDatabase documentDatabase;
        private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;

        public RavenDBOptions(InMemoryRavenConfiguration configuration)
        {
            //TODO should we do HttpEndpointRegistration.RegisterHttpEndpointTarget(); here?
            documentDatabase = new DocumentDatabase(configuration);
            try
            {
                documentDatabase.SpinBackgroundWorkers();
                databasesLandlord = new DatabasesLandlord(documentDatabase);
                mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
                mixedModeRequestAuthorizer.Initialize(documentDatabase,
                    new RavenServer(databasesLandlord.SystemDatabase, configuration));
            }
            catch
            {
                documentDatabase.Dispose();
                throw;
            }
        }

        public DocumentDatabase DocumentDatabase
        {
            get { return documentDatabase; }
        }

        public MixedModeRequestAuthorizer MixedModeRequestAuthorizer
        {
            get { return mixedModeRequestAuthorizer; }
        }

        public DatabasesLandlord Landlord
        {
            get { return databasesLandlord; }
        }

        private class RavenServer : IRavenServer
        {
            private readonly InMemoryRavenConfiguration systemConfiguration;
            private readonly DocumentDatabase systemDatabase;

            public RavenServer(DocumentDatabase systemDatabase, InMemoryRavenConfiguration systemConfiguration)
            {
                this.systemDatabase = systemDatabase;
                this.systemConfiguration = systemConfiguration;
            }

            public DocumentDatabase SystemDatabase
            {
                get { return systemDatabase; }
            }

            public InMemoryRavenConfiguration SystemConfiguration
            {
                get { return systemConfiguration; }
            }
        }
    }
}