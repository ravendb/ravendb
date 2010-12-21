//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using System.Linq;
using Raven.Database.Storage;
using Raven.Http.Exceptions;
using Raven.Storage.Esent.Backup;
using Raven.Storage.Esent.SchemaUpdates;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent
{
	public class TransactionalStorage : CriticalFinalizerObject, ITransactionalStorage
	{
		private readonly ThreadLocal<StorageActionsAccessor> current = new ThreadLocal<StorageActionsAccessor>();
		private readonly string database;
        private readonly InMemoryRavenConfiguration configuration;
		private readonly Action onCommit;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly string path;
		private bool disposed;

		private JET_INSTANCE instance;
		private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
	    private IUuidGenerator generator;

	    [ImportMany]
		public IEnumerable<ISchemaUpdate> Updaters { get; set; }

		[ImportMany]
		public IEnumerable<AbstractDocumentCodec> DocumentCodecs { get; set; }

        public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			database = configuration.DataDirectory;
			this.configuration = configuration;
			this.onCommit = onCommit;
			path = database;
			if (Path.IsPathRooted(database) == false)
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
			database = Path.Combine(path, "Data");

			new TransactionalStorageConfigurator(configuration).LimitSystemCache();

			Api.JetCreateInstance(out instance, database + Guid.NewGuid());
		}

		public TableColumnsCache TableColumnsCache
		{
			get { return tableColumnsCache; }
		}

		public JET_INSTANCE Instance
		{
			get { return instance; }
		}

		public string Database
		{
			get { return database; }
		}

		public Guid Id { get; private set; }

		#region IDisposable Members

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;
				GC.SuppressFinalize(this);
				Api.JetTerm2(instance, TermGrbit.Complete);
			}
			finally
			{
				disposed = true;
				disposerLock.ExitWriteLock();
			}
		}

		public void StartBackupOperation(DocumentDatabase docDb, string backupDestinationDirectory)
		{
			var backupOperation = new BackupOperation(docDb, docDb.Configuration.DataDirectory, backupDestinationDirectory);
			ThreadPool.QueueUserWorkItem(backupOperation.Execute);
		}

		public void Restore(string backupLocation, string databaseLocation)
		{
			new RestoreOperation(backupLocation, databaseLocation).Execute();
		}

	    public Type TypeForRunningQueriesInRemoteAppDomain
	    {
	        get { return typeof(RemoteEsentStorage); }
	    }

	    public object StateForRunningQueriesInRemoteAppDomain
	    {
            get
            {
                return new RemoteEsentStorageState
                {
                    Database = database,
                    Instance = instance
                };
            }
	    }

	    public bool HandleException(Exception exception)
	    {
	        var e = exception as EsentErrorException;
            if (e == null)
                return false;
            // we need to protect ourselve from rollbacks happening in an async manner
            // after the database was already shut down.
	        return e.Error == JET_err.InvalidInstance;
	    }

	    #endregion

        public bool Initialize(IUuidGenerator uuidGenerator)
		{
			try
			{
			    generator = uuidGenerator;
				new TransactionalStorageConfigurator(configuration).ConfigureInstance(instance, path);

				if (configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
				{
					new InstanceParameters(instance)
					{
						CircularLog = true,
						Recovery = false,
						NoInformationEvent = false,
						CreatePathIfNotExist = true,
						TempDirectory = Path.Combine(path, "temp"),
						SystemDirectory = Path.Combine(path, "system"),
						LogFileDirectory = Path.Combine(path, "logs"),
						MaxVerPages = 128,
						BaseName = "RVN",
						EventSource = "Raven",
						LogBuffers = 8192,
						LogFileSize = 256,
						MaxSessions = TransactionalStorageConfigurator.MaxSessions,
						MaxCursors = 1024,
						DbExtensionSize = 128,
						AlternateDatabaseRecoveryDirectory = path
					};
				}

				Api.JetInit(ref instance);

				var newDb = EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				tableColumnsCache.InitColumDictionaries(instance, database);

				return newDb;
			}
			catch (Exception e)
			{
				Dispose();
				throw new InvalidOperationException("Could not open transactional storage: " + database, e);
			}
		}

		private void SetIdFromDb()
		{
			try
			{
				instance.WithDatabase(database, (session, dbid) =>
				{
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
					{
						Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
						var columnids = Api.GetColumnDictionary(session, details);
						var column = Api.RetrieveColumn(session, details, columnids["id"]);
						Id = new Guid(column);
						var schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
						if (schemaVersion == SchemaCreator.SchemaVersion)
							return;
						do
						{
							var updater = Updaters.FirstOrDefault(update => update.FromSchemaVersion == schemaVersion);
							if (updater == null)
								throw new InvalidOperationException(string.Format("The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", schemaVersion, SchemaCreator.SchemaVersion, Environment.NewLine));
                            updater.Init(generator);
							updater.Update(session, dbid);
							schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
						} while (schemaVersion != SchemaCreator.SchemaVersion);
					}
				});
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." +
						Environment.NewLine +
							"You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
					e);
			}
		}

		private bool EnsureDatabaseIsCreatedAndAttachToDatabase()
		{
			using (var session = new Session(instance))
			{
				try
				{
					Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
					return false;
				}
				catch (EsentErrorException e)
				{
					if (e.Error == JET_err.DatabaseDirtyShutdown)
					{
						try
						{
							using (var recoverInstance = new Instance("Recovery instance for: " + database))
							{
								recoverInstance.Init();
								using (var recoverSession = new Session(recoverInstance))
								{
									new TransactionalStorageConfigurator(configuration).ConfigureInstance(recoverInstance.JetInstance, path);
									Api.JetAttachDatabase(recoverSession, database,
														  AttachDatabaseGrbit.DeleteCorruptIndexes);
									Api.JetDetachDatabase(recoverSession, database);
								}
							}
						}
						catch (Exception)
						{
						}

						Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						return false;
					}
					if (e.Error != JET_err.FileNotFound)
						throw;
				}

				new SchemaCreator(session).Create(database);
				Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
				return true;
			}
		}

		~TransactionalStorage()
		{
			try
			{
				Trace.WriteLine(
					"Disposing esent resources from finalizer! You should call TransactionalStorage.Dispose() instead!");
				Api.JetTerm2(instance, TermGrbit.Abrupt);
			}
			catch (Exception exception)
			{
				try
				{
					Trace.WriteLine("Failed to dispose esent instance from finalizer because: " + exception);
				}
				catch
				{
				}
			}
		}

		[CLSCompliant(false)]
		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (disposed)
			{
				Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
				return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
			}
			if (current.Value != null)
			{
				action(current.Value);
				return;
			}
			disposerLock.EnterReadLock();
			try
			{
				ExecuteBatch(action);
			}
			catch (EsentErrorException e)
			{
				switch (e.Error)
				{
					case JET_err.WriteConflict:
					case JET_err.SessionWriteConflict:
					case JET_err.WriteConflictPrimaryIndex:
						throw new ConcurrencyException("Concurrent modification to the same document are not allowed");
					default:
						throw;
				}
			}
			finally
			{
				disposerLock.ExitReadLock();
				current.Value = null;
			}
		}

        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
        private void ExecuteBatch(Action<IStorageActionsAccessor> action)
		{
			var txMode = configuration.TransactionMode == TransactionMode.Lazy
				? CommitTransactionGrbit.LazyFlush
				: CommitTransactionGrbit.None;
			using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator))
			{
				current.Value = new StorageActionsAccessor(pht);
				action(current.Value);
				pht.Commit(txMode);
				onCommit();
			}
		}

		public void ExecuteImmediatelyOrRegisterForSyncronization(Action action)
		{
			if (current.Value == null)
			{
				action();
				return;
			}
			current.Value.OnCommit += action;
		}

		internal StorageActionsAccessor GetCurrentBatch()
		{
			var batch = current.Value;
			if (batch == null)
				throw new InvalidOperationException("Batch was not started, you are not supposed to call this method");
			return batch;
		}
	}
}
