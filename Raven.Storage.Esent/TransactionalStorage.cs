using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Plugins;
using Raven.Database.Storage.SchemaUpdates;
using System.Linq;
using Raven.Database.Storage.StorageActions;

namespace Raven.Database.Storage
{
	public class TransactionalStorage : CriticalFinalizerObject, IDisposable, ITransactionalStorage
	{
		public const int MaxSessions = 256;
		private readonly ThreadLocal<StorageActionsAccessor> current = new ThreadLocal<StorageActionsAccessor>();
		private readonly string database;
		private readonly RavenConfiguration configuration;
		private readonly Action onCommit;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly string path;
		private bool disposed;

		private JET_INSTANCE instance;
		private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();

		[ImportMany]
		public IEnumerable<ISchemaUpdate> Updaters { get; set; }

		[ImportMany]
		public IEnumerable<AbstractDocumentCodec> DocumentCodecs { get; set; }

		public TransactionalStorage(RavenConfiguration configuration, Action onCommit)
		{
			database = configuration.DataDirectory;
			this.configuration = configuration;
			this.onCommit = onCommit;
			path = database;
			if (Path.IsPathRooted(database) == false)
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
			database = Path.Combine(path, "Data");

			LimitSystemCache();

			Api.JetCreateInstance(out instance, database + Guid.NewGuid());
		}

		public TableColumnsCache TableColumnsCache
		{
			get { return tableColumnsCache; }
		}

		private static void LimitSystemCache()
		{
			var cacheSizeMax = 1024 * 1024 * 1024 / SystemParameters.DatabasePageSize;
			if (SystemParameters.CacheSizeMax > cacheSizeMax)
			{
				SystemParameters.CacheSizeMax = cacheSizeMax; // 1 GB
			}
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

		#endregion

		public bool Initialize()
		{
			try
			{
				ConfigureInstance(instance, path);

				if(configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction)
				{
					new InstanceParameters(instance)
					{
						Recovery = false,
						CircularLog = false,
					};
				}

				Api.JetInit(ref instance);

				var newDb = EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				InitColumDictionaries();

				return newDb;
			}
			catch (Exception e)
			{
				Dispose();
				throw new InvalidOperationException("Could not open transactional storage: " + database, e);
			}
		}

		private void InitColumDictionaries()
		{
			using (var session = new Session(instance))
			{
				var dbid = JET_DBID.Nil;
				try
				{
					Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
					using (var documents = new Table(session, dbid, "documents", OpenTableGrbit.None))
						tableColumnsCache.DocumentsColumns = Api.GetColumnDictionary(session, documents);
					using (var tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None))
						tableColumnsCache.TasksColumns = Api.GetColumnDictionary(session, tasks);
					using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
						tableColumnsCache.FilesColumns = Api.GetColumnDictionary(session, files);
					using (var indexStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
						tableColumnsCache.IndexesStatsColumns = Api.GetColumnDictionary(session, indexStats);
					using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
						tableColumnsCache.MappedResultsColumns = Api.GetColumnDictionary(session, mappedResults);
					using (var documentsModifiedByTransactions = new Table(session, dbid, "documents_modified_by_transaction", OpenTableGrbit.None))
						tableColumnsCache.DocumentsModifiedByTransactionsColumns = Api.GetColumnDictionary(session, documentsModifiedByTransactions);
					using (var transactions = new Table(session, dbid, "transactions", OpenTableGrbit.None))
						tableColumnsCache.TransactionsColumns = Api.GetColumnDictionary(session, transactions);
					using (var identity = new Table(session, dbid, "identity_table", OpenTableGrbit.None))
						tableColumnsCache.IdentityColumns = Api.GetColumnDictionary(session, identity);
					using (var details = new Table(session, dbid, "details", OpenTableGrbit.None))
						tableColumnsCache.DetailsColumns = Api.GetColumnDictionary(session, details);
                    using (var queue = new Table(session, dbid, "queue", OpenTableGrbit.None))
                        tableColumnsCache.QueueColumns = Api.GetColumnDictionary(session, queue);
				}
				finally
				{
					if (Equals(dbid, JET_DBID.Nil) == false)
						Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
				}
			}
		}

		public static void ConfigureInstance(JET_INSTANCE jetInstance, string path)
		{
			path = Path.GetFullPath(path);
			new InstanceParameters(jetInstance)
			{
				CircularLog = true,
				Recovery = true,
				NoInformationEvent = false,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(path, "temp"),
				SystemDirectory = Path.Combine(path, "system"),
				LogFileDirectory = Path.Combine(path, "logs"),
				MaxVerPages = 4096, // 256 MB
				BaseName = "RVN",
				EventSource = "Raven",
				LogBuffers = 256,
				LogFileSize = 16384,
				MaxSessions = MaxSessions,
				MaxCursors = 2048,
				DbExtensionSize = 4096,
				AlternateDatabaseRecoveryDirectory = path
			};
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
							updater.Update(session, dbid);
							schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
						}while(schemaVersion != SchemaCreator.SchemaVersion);
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
									ConfigureInstance(recoverInstance.JetInstance, path);
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
			finally
			{
				disposerLock.ExitReadLock();
				current.Value = null;
			}
		}

		private void ExecuteBatch(Action<IStorageActionsAccessor> action)
		{
			var txMode = configuration.TransactionMode == TransactionMode.Lazy
				? CommitTransactionGrbit.LazyFlush
				: CommitTransactionGrbit.None;
			using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, DocumentCodecs))
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