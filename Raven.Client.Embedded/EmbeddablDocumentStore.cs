using System.Configuration;
using System.Net;
using Raven.Client.Document;
using Raven.Database;

namespace Raven.Client.Client
{
    public class EmbeddablDocumentStore : DocumentStore
    {
        private RavenConfiguration configuration;

        public override string Identifier
        {
            get { return base.Identifier ?? (RunInMemory ? "memory" : DataDirectory); }
            set { base.Identifier = value; }
        }

        public RavenConfiguration Configuration
        {
            get
            {
                if (configuration == null)
                    configuration = new RavenConfiguration();
                return configuration;
            }
            set { configuration = value; }
        }


        /// <summary>
        /// Run RavenDB in an embedded mode, using in memory only storage.
        /// This is useful for unit tests, since it is very fast.
        /// </summary>
        public bool RunInMemory
        {
            get { return Configuration.RunInMemory; }
            set
            {
                Configuration.RunInMemory = true;
                Configuration.StorageTypeName = "Raven.Storage.Managed.TransactionalStorage, Raven.Storage.Managed";
            }
        }

        /// <summary>
        /// Run RavenDB in embedded mode, using the specified directory for data storage
        /// </summary>
        /// <value>The data directory.</value>
        public string DataDirectory
        {
            get { return Configuration.DataDirectory; }
            set { Configuration.DataDirectory = value; }
        }

        public DocumentDatabase DocumentDatabase { get; set; }

        protected override void ProcessConnectionStringOption(NetworkCredential neworkCredentials, string key,
                                                             string value)
        {
            switch (key)
            {
                case "memory":
                    bool result;
                    if (bool.TryParse(value, out result) == false)
                        throw new ConfigurationErrorsException("Could not understand memory setting: " +
                                                               value);
                    RunInMemory = result;
                    break;
                case "datadir":
                    DataDirectory = value;
                    break;
                default:
                    base.ProcessConnectionStringOption(neworkCredentials, key, value);
                    break;
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            if (DocumentDatabase != null)
                DocumentDatabase.Dispose();
        }

        protected override void InitializeInternal()
        {
            if (configuration != null)
            {
                DocumentDatabase = new DocumentDatabase(configuration);
                DocumentDatabase.SpinBackgroundWorkers();
                databaseCommandsGenerator = () => new EmbededDatabaseCommands(DocumentDatabase, Conventions);
            }
            else
            {
                base.InitializeInternal();
            }
        }
    }
}