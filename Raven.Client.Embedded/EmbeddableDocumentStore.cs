//-----------------------------------------------------------------------
// <copyright file="EmbeddableDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Configuration;
using System.Net;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;

namespace Raven.Client.Embedded
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// Also supports hosting RavenDB in an embedded mode
    /// </summary>
    public class EmbeddableDocumentStore : DocumentStore
    {
        private RavenConfiguration configuration;
        private RavenDbHttpServer httpServer;
        private bool wasDisposed;

		/// <summary>
		/// Raised after this instance has been disposed
		/// </summary>
    	public event Action Disposed = delegate { };

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public override string Identifier
        {
            get { return base.Identifier ?? (RunInMemory ? "memory #" + GetHashCode() : DataDirectory); }
            set { base.Identifier = value; }
        }

        ///<summary>
        /// Get or set the configuration instance for embedded RavenDB
        ///</summary>
        public RavenConfiguration Configuration
        {
            get { return configuration ?? (configuration = new RavenConfiguration()); }
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
                Configuration.RunInMemory = value;
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

        ///<summary>
        /// Access the embedded instance of RavenDB
        ///</summary>
        public DocumentDatabase DocumentDatabase { get; private set; }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            if (wasDisposed)
                return;
            wasDisposed = true;
            base.Dispose();
            if (DocumentDatabase != null)
                DocumentDatabase.Dispose();
            if (httpServer != null)
                httpServer.Dispose();


        	var onDisposed = Disposed;
			if (onDisposed != null)
				onDisposed();
        }

    	/// <summary>
    	/// Create the connection string parser
    	/// </summary>
    	protected override Abstractions.Data.ConnectionStringParser CreateConnectionStringParser(string connectionString)
		{
			var connectionStringParser = base.CreateConnectionStringParser(connectionString);
			connectionStringParser.AllowEmbeddedOptions = true;
			return connectionStringParser;
		}

		protected override void GetConnectionStringSettings(Abstractions.Data.ConnectionStringParser connectionStringParser)
		{
			base.GetConnectionStringSettings(connectionStringParser);
			RunInMemory = connectionStringParser.RunInMemory;
			DataDirectory = connectionStringParser.DataDirectory;
		}


    	/// <summary>
        /// Initialize the document store access method to RavenDB
        /// </summary>
        protected override void InitializeInternal()
        {
            if (configuration != null && Url == null)
            {
				if(configuration.RunInMemory || configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
				{
					ResourceManagerId = Guid.NewGuid(); // avoid conflicts
				}
                DocumentDatabase = new DocumentDatabase(configuration);
                DocumentDatabase.SpinBackgroundWorkers();
                if (UseEmbeddedHttpServer)
                {
                    httpServer = new RavenDbHttpServer(configuration, DocumentDatabase);
                    httpServer.Start();
                }
                databaseCommandsGenerator = () => new EmbeddedDatabaseCommands(DocumentDatabase, Conventions);
            }
            else
            {
                base.InitializeInternal();
            }
        }

        ///<summary>
        /// Whatever we should also host an HTTP endpoint for the document database
        ///</summary>
        public bool UseEmbeddedHttpServer { get; set; }
    }
}