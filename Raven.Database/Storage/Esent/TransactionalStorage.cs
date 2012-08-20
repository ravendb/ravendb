//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.MEF;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using System.Linq;
using Raven.Database.Storage;
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
		private volatile bool disposed;

		private JET_INSTANCE instance;
		private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
		private IUuidGenerator generator;
		private readonly IDocumentCacher documentCacher;

		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		[ImportMany]
		public OrderedPartCollection<ISchemaUpdate> Updaters { get; set; }

		static TransactionalStorage()
		{
			try
			{
				SystemParameters.MaxInstances = 1024;
			}
			catch (EsentErrorException e)
			{
				// this is expected if we had done something like recycyling the app domain
				// because the engine state is actually at the process level (unmanaged)
				// so we ignore this error
				if (e.Error == JET_err.AlreadyInitialized)
					return;
				throw;
			}
		}

		public TransactionalStorage(InMemoryRavenConfiguration configuration, Action onCommit)
		{
			Console.WriteLine("esent");
			configuration.Container.SatisfyImportsOnce(this);
			documentCacher = new DocumentCacher(configuration);
			database = configuration.DataDirectory;
			this.configuration = configuration;
			this.onCommit = onCommit;
			path = database;
			if (Path.IsPathRooted(database) == false)
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
			database = Path.Combine(path, "Data");

			RecoverFromFailedCompact(database);

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

		

		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;
				
				disposed = true;
				GC.SuppressFinalize(this);
				current.Dispose();
				if (documentCacher != null)
					documentCacher.Dispose();
				Api.JetTerm2(instance, TermGrbit.Complete);
			}
			finally
			{
				disposerLock.ExitWriteLock();
			}
		}

		public void StartBackupOperation(DocumentDatabase docDb, string backupDestinationDirectory, bool incrementalBackup)
		{
			var backupOperation = new BackupOperation(docDb, docDb.Configuration.DataDirectory, backupDestinationDirectory, incrementalBackup);
			ThreadPool.QueueUserWorkItem(backupOperation.Execute);
		}

		public void Restore(string backupLocation, string databaseLocation)
		{
			new RestoreOperation(backupLocation, databaseLocation).Execute();
		}

		public long GetDatabaseSizeInBytes()
		{
			long sizeInBytes;

			using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator, documentCacher, this))
			{
				int sizeInPages, pageSize;
				Api.JetGetDatabaseInfo(pht.Session, pht.Dbid, out sizeInPages, JET_DbInfo.Filesize);
				Api.JetGetDatabaseInfo(pht.Session, pht.Dbid, out pageSize, JET_DbInfo.PageSize);
				sizeInBytes = ((long)sizeInPages) * pageSize;
			}

			return sizeInBytes;

		}

		public string FriendlyName
		{
			get { return "Esent"; }
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

		private static void RecoverFromFailedCompact(string file)
		{
			string renamedFile = file + ".RenameOp";
			if (File.Exists(renamedFile) == false) // not in the middle of compact op, we are good
				return;

			if (File.Exists(file))
			// we successfully renamed the new file and crashed before we could remove the old copy
			{
				//just complete the op and we are good (committed)
				File.Delete(renamedFile);
			}
			else // we successfully renamed the old file and crashed before we could remove the new file
			{
				// just undo the op and we are good (rollback)
				File.Move(renamedFile, file);
			}
		}

		public void Compact(InMemoryRavenConfiguration ravenConfiguration)
		{
			var src = Path.Combine(ravenConfiguration.DataDirectory, "Data");
			var compactPath = Path.Combine(ravenConfiguration.DataDirectory, "Data.Compact");
			
			if(File.Exists(compactPath))
				File.Delete(compactPath);
			RecoverFromFailedCompact(src);

			JET_INSTANCE compactInstance;
			Api.JetCreateInstance(out compactInstance, ravenConfiguration.DataDirectory + Guid.NewGuid());
			try
			{
				new TransactionalStorageConfigurator(ravenConfiguration)
					.ConfigureInstance(compactInstance, ravenConfiguration.DataDirectory);
				Api.JetInit(ref compactInstance);
				using(var session = new Session(compactInstance))
				{
					Api.JetAttachDatabase(session, src, AttachDatabaseGrbit.None);
					try
					{
						Api.JetCompact(session, src, compactPath, null, null,
								   CompactGrbit.None);
					}
					finally
					{
						Api.JetDetachDatabase(session, src);
					}
				}
			}
			finally
			{
				Api.JetTerm2(compactInstance, TermGrbit.Complete);
			}

			File.Move(src, src +".RenameOp");
			File.Move(compactPath, src);
			File.Delete(src + ".RenameOp");

		}

		public bool Initialize(IUuidGenerator uuidGenerator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
		{
			try
			{
				DocumentCodecs = documentCodecs;
				generator = uuidGenerator;
				var instanceParameters = new TransactionalStorageConfigurator(configuration).ConfigureInstance(instance, path);

				if (configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
				{
					instanceParameters = new InstanceParameters(instance)
					{
						CircularLog = true,
						Recovery = false,
						NoInformationEvent = false,
						CreatePathIfNotExist = true,
						TempDirectory = Path.Combine(path, "temp"),
						SystemDirectory = Path.Combine(path, "system"),
						LogFileDirectory = Path.Combine(path, "logs"),
						MaxVerPages = 256,
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

				log.Info(@"Esent Settings:
  MaxVerPages      = {0}
  CacheSizeMax     = {1}
  DatabasePageSize = {2}", instanceParameters.MaxVerPages, SystemParameters.CacheSizeMax, SystemParameters.DatabasePageSize);

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

		protected OrderedPartCollection<AbstractDocumentCodec> DocumentCodecs { get; set; }

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
							var updater = Updaters.FirstOrDefault(update => update.Value.FromSchemaVersion == schemaVersion);
							if (updater == null)
								throw new InvalidOperationException(string.Format("The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", schemaVersion, SchemaCreator.SchemaVersion, Environment.NewLine));
							updater.Value.Init(generator);
							updater.Value.Update(session, dbid);
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
		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (disposerLock.IsReadLockHeld) // we are currently in a nested Batch call
			{
				if (current.Value != null) // check again, just to be sure
				{
					action(current.Value);
					return;
				}
			}
			disposerLock.EnterReadLock();
			try
			{
				ExecuteBatch(action);
			}
			catch (EsentErrorException e)
			{
				if (disposed)
				{
					Trace.WriteLine("TransactionalStorage.Batch was called after it was disposed, call was ignored.");
					return; // this may happen if someone is calling us from the finalizer thread, so we can't even throw on that
				}
			
				switch (e.Error)
				{
					case JET_err.WriteConflict:
					case JET_err.SessionWriteConflict:
					case JET_err.WriteConflictPrimaryIndex:
						throw new ConcurrencyException("Concurrent modification to the same document are not allowed", e);
					default:
						throw;
				}
			}
			finally
			{
				disposerLock.ExitReadLock();
				if(disposed == false)
					current.Value = null;
			}
			onCommit(); // call user code after we exit the lock
		}

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		private void ExecuteBatch(Action<IStorageActionsAccessor> action)
		{
			var txMode = configuration.TransactionMode == TransactionMode.Lazy
				? CommitTransactionGrbit.LazyFlush
				: CommitTransactionGrbit.None;
			using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs, generator, documentCacher, this))
			{
				var storageActionsAccessor = new StorageActionsAccessor(pht);
				current.Value = storageActionsAccessor;
				action(current.Value);
				storageActionsAccessor.SaveAllTasks();
				pht.Commit(txMode);
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
