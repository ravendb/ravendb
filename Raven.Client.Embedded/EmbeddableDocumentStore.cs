//-----------------------------------------------------------------------
// <copyright file="EmbeddableDocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Embedded.Changes;
using Raven.Client.Util;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Extensions;

namespace Raven.Client.Embedded
{
	/// <summary>
	/// Manages access to RavenDB and open sessions to work with RavenDB.
	/// Also supports hosting RavenDB in an embedded mode
	/// </summary>
	public class EmbeddableDocumentStore : DocumentStore
	{
		ILog log = Raven.Abstractions.Logging.LogProvider.GetCurrentClassLogger();

		static EmbeddableDocumentStore()
		{
			HttpEndpointRegistration.RegisterHttpEndpointTarget();
		}

		private RavenConfiguration configuration;
		private HttpServer httpServer;
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
			get
			{
				return Configuration.DataDirectory;
			}
			set { Configuration.DataDirectory = value; }
		}

		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		public override string Url
		{
			get
			{
				return base.Url;
			}
			set
			{
				DataDirectory = null;
				base.Url = value;
			}
		}

		private EmbeddableDatabaseChanges databaseChanges;
		private Timer idleTimer;

		/// <summary>
		/// Subscribe to change notifications from the server
		/// </summary>
		public override IDatabaseChanges Changes(string database = null)
		{
			if(string.IsNullOrEmpty(Url) == false)
				return base.Changes(database);

			if(database != null)
				throw new NotSupportedException("Embedded document store does not support multi tenancy");

			if(databaseChanges == null)
			{
				lock(this)
				{
					Thread.MemoryBarrier();
					if(databaseChanges == null)
						databaseChanges = new EmbeddableDatabaseChanges(this, () => databaseChanges = null);
				}
			}
			return databaseChanges;
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
			if (idleTimer != null)
				idleTimer.Dispose();
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
		protected override RavenConnectionStringOptions GetConnectionStringOptions()
		{
			var parser = ConnectionStringParser<EmbeddedRavenConnectionStringOptions>.FromConnectionStringName(ConnectionStringName);
			parser.Parse();
			return parser.ConnectionStringOptions;
		}

		/// <summary>
		/// Copy the relevant connection string settings
		/// </summary>
		protected override void SetConnectionStringSettings(RavenConnectionStringOptions options)
		{
			base.SetConnectionStringSettings(options);
			var embeddedRavenConnectionStringOptions = options as EmbeddedRavenConnectionStringOptions;

			if (embeddedRavenConnectionStringOptions == null)
				return;

			if (string.IsNullOrEmpty(embeddedRavenConnectionStringOptions.DataDirectory) == false)
				DataDirectory = embeddedRavenConnectionStringOptions.DataDirectory;

			RunInMemory = embeddedRavenConnectionStringOptions.RunInMemory;

		}


		/// <summary>
		/// Initialize the document store access method to RavenDB
		/// </summary>
		protected override void InitializeInternal()
		{
			if (string.IsNullOrEmpty(Url) == false && string.IsNullOrEmpty(DataDirectory) == false)
				throw new InvalidOperationException("You cannot specify both Url and DataDirectory at the same time. Url implies running in client/server mode against the remote server. DataDirectory implies running in embedded mode. Those two options are incompatible");

			if (string.IsNullOrEmpty(DataDirectory) == false && string.IsNullOrEmpty(DefaultDatabase) == false)
				throw new InvalidOperationException("You cannot specify DefaultDatabase value when the DataDirectory has been set, running in Embedded mode, the Default Database is not a valid option.");

			if (configuration != null && Url == null)
			{
				configuration.PostInit();
				if (configuration.RunInMemory || configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
				{
					ResourceManagerId = Guid.NewGuid(); // avoid conflicts
				}
				DocumentDatabase = new DocumentDatabase(configuration);
				DocumentDatabase.SpinBackgroundWorkers();
				if (UseEmbeddedHttpServer)
				{
					httpServer = new HttpServer(configuration, DocumentDatabase);
					httpServer.StartListening();
				}
				else // we need to setup our own idle timer
				{
					idleTimer = new Timer(state =>
					{
						try
						{
							DocumentDatabase.RunIdleOperations();
						}
						catch (Exception e)
						{
							log.WarnException("Error during database idle operations", e);
						}
					},null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
				}
				databaseCommandsGenerator = () => new EmbeddedDatabaseCommands(DocumentDatabase, Conventions, currentSessionId);
			}
			else
			{
				base.InitializeInternal();
			}
		}


		/// <summary>
		/// validate the configuration for the document store
		/// </summary>
		protected override void AssertValidConfiguration()
		{
			if (RunInMemory)
				return;
			if (string.IsNullOrEmpty(DataDirectory))  // if we don't have a data dir...
				base.AssertValidConfiguration();	 // we need to check the configuration for url

		}


		/// <summary>
		/// Expose the internal http server, if used
		/// </summary>
		public HttpServer HttpServer
		{
			get { return httpServer; }
		}

		///<summary>
		/// Whatever we should also host an HTTP endpoint for the document database
		///</summary>
		public bool UseEmbeddedHttpServer { get; set; }
	}
}