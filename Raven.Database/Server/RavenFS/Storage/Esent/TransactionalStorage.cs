//-----------------------------------------------------------------------
// <copyright file="TransactionalStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Isam.Esent.Interop;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.RavenFS.Storage.Esent.Backup;
using Raven.Json.Linq;

using Voron.Impl.Backup;

using BackupOperation = Raven.Database.Server.RavenFS.Storage.Esent.Backup.BackupOperation;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
	public class TransactionalStorage : CriticalFinalizerObject, ITransactionalStorage
	{
		private readonly InMemoryRavenConfiguration configuration;

		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private readonly string database;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly string path;
		private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
		private bool disposed;
		private readonly ILog log = LogManager.GetCurrentClassLogger();
		private JET_INSTANCE instance;

		static TransactionalStorage()
		{
			try
			{
				SystemParameters.MaxInstances = 1024;
			}
			catch (EsentErrorException e)
			{
				if (e.Error != JET_err.AlreadyInitialized)
					throw;
			}
		}

		public TransactionalStorage(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;
			path = configuration.FileSystem.DataDirectory.ToFullPath();
			database = Path.Combine(path, "Data.ravenfs");

			RecoverFromFailedCompact(database);

			new TransactionalStorageConfigurator(configuration).LimitSystemCache();

			CreateInstance(out instance, database + Guid.NewGuid());
		}

        public string FriendlyName
        {
            get { return "Esent"; }
        }

		public TableColumnsCache TableColumnsCache
		{
			get { return tableColumnsCache; }
		}

		public JET_INSTANCE Instance
		{
			get { return instance; }
		}

		public Guid Id { get; private set; }


		public void Dispose()
		{
			disposerLock.EnterWriteLock();
			try
			{
				if (disposed)
					return;
				GC.SuppressFinalize(this);
				try
				{
					Api.JetTerm2(instance, TermGrbit.Complete);
				}
				catch (Exception e)
				{
					log.WarnException("Could not do gracefully disposal of RavenFS", e);
					try
					{
						Api.JetTerm2(instance, TermGrbit.Abrupt);
					}
					catch (Exception e2)
					{
						log.FatalException("Even ungraceful shutdown was unsuccessful, restarting the server process may be required", e2);
					}
				}
			}
			finally
			{
				disposed = true;
				disposerLock.ExitWriteLock();
			}
		}

		public void Initialize()
		{
			try
			{
				new TransactionalStorageConfigurator(configuration).ConfigureInstance(instance, path);

				Api.JetInit(ref instance);

				EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				tableColumnsCache.InitColumDictionaries(instance, database);
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
						var schemaVersion = Api.RetrieveColumnAsString(session, details,
																	   columnids["schema_version"]);
						if (schemaVersion == SchemaCreator.SchemaVersion)
							return;
						throw new InvalidOperationException(
							string.Format(
								"The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
								schemaVersion, SchemaCreator.SchemaVersion, Environment.NewLine));
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
			try
			{
				using (var session = new Session(instance))
				{
					Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
				}
				return false;
			}
			catch (EsentErrorException e)
			{
				switch (e.Error)
				{
					case JET_err.SecondaryIndexCorrupted:
						Output("Secondary Index Corrupted detected, attempting to compact...");
						Api.JetTerm2(instance, TermGrbit.Complete);
						Compact(configuration, (sesid, snp, snt, data) =>
						{
							Output(string.Format("{0}, {1}, {2}, {3}", sesid, snp, snt, data));
							return JET_err.Success;
						});
						CreateInstance(out instance, database + Guid.NewGuid());
						Api.JetInit(ref instance);
						using (var session = new Session(instance))
						{
							Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						}
						return false;
					case JET_err.DatabaseDirtyShutdown:
						try
						{
							Api.JetTerm2(instance, TermGrbit.Complete);
							using (var recoverInstance = new Instance("Recovery instance for: " + database))
							{
								new TransactionalStorageConfigurator(configuration).ConfigureInstance(recoverInstance.JetInstance, path);
								recoverInstance.Init();
								using (var recoverSession = new Session(recoverInstance))
								{
									Api.JetAttachDatabase(recoverSession, database,
														  AttachDatabaseGrbit.DeleteCorruptIndexes);
									Api.JetDetachDatabase(recoverSession, database);
								}
							}
						}
						catch (Exception e2)
						{
							log.WarnException("Could not recover from dirty shutdown in RavenFS " + database, e2);
						}
						CreateInstance(out instance, database + Guid.NewGuid());
						Api.JetInit(ref instance);
						using (var session = new Session(instance))
						{
							Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						}
						return false;
				}
				if (e.Error != JET_err.FileNotFound)
					throw;
			}

			using (var session = new Session(instance))
			{
				new SchemaCreator(session).Create(database);
				Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
				return true;
			}

		}

		~TransactionalStorage()
		{
			try
			{
				Trace.WriteLine("Disposing esent resources from finalizer! You should call Storage.Dispose() instead!");
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

		[DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough]
		public void Batch(Action<IStorageActionsAccessor> action)
		{
			if (Id == Guid.Empty)
				throw new InvalidOperationException("Cannot use Storage before Initialize was called");
			if (disposed)
			{
				Trace.WriteLine("Storage.Batch was called after it was disposed, call was ignored.");
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
						throw new ConcurrencyException("Concurrent modification to the same file are not allowed", e);
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
			if (current.Value != null)
			{
				action(current.Value);
				return;
			}

			try
			{
				using (var storageActionsAccessor = new StorageActionsAccessor(tableColumnsCache, instance, database))
				{
					current.Value = storageActionsAccessor;

					action(storageActionsAccessor);
					storageActionsAccessor.Commit();
				}
			}
			finally
			{
				current.Value = null;
			}
		}

		public static void Compact(InMemoryRavenConfiguration ravenConfiguration, JET_PFNSTATUS statusCallback)
		{
			var src = Path.Combine(ravenConfiguration.FileSystem.DataDirectory.ToFullPath(), "Data.ravenfs");
			var compactPath = Path.Combine(ravenConfiguration.FileSystem.DataDirectory.ToFullPath(), "Data.ravenfs.Compact");

			if (File.Exists(compactPath))
				File.Delete(compactPath);
			RecoverFromFailedCompact(src);


			JET_INSTANCE compactInstance;
			CreateInstance(out compactInstance, ravenConfiguration.FileSystem.DataDirectory + Guid.NewGuid());
			try
			{
				new TransactionalStorageConfigurator(ravenConfiguration)
					.ConfigureInstance(compactInstance, ravenConfiguration.FileSystem.DataDirectory);
				DisableIndexChecking(compactInstance);
				Api.JetInit(ref compactInstance);
				using (var session = new Session(compactInstance))
				{
					Api.JetAttachDatabase(session, src, AttachDatabaseGrbit.None);
					try
					{
						Api.JetCompact(session, src, compactPath, statusCallback, null,
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

			File.Move(src, src + ".RenameOp");
			File.Move(compactPath, src);
			File.Delete(src + ".RenameOp");

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

		public static void CreateInstance(out JET_INSTANCE compactInstance, string name)
		{
			Api.JetCreateInstance(out compactInstance, name);

			DisableIndexChecking(compactInstance);
		}

		public static void DisableIndexChecking(JET_INSTANCE jetInstance)
		{
			Api.JetSetSystemParameter(jetInstance, JET_SESID.Nil, JET_param.EnableIndexChecking, 0, null);
			if (Environment.OSVersion.Version >= new Version(5, 2))
			{
				// JET_paramEnableIndexCleanup is not supported on WindowsXP

				const int JET_paramEnableIndexCleanup = 54;

				Api.JetSetSystemParameter(jetInstance, JET_SESID.Nil, (JET_param)JET_paramEnableIndexCleanup, 0, null);
			}
		}

        public void StartBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupDestinationDirectory, bool incrementalBackup, FileSystemDocument fileSystemDocument)
        {
            if (new InstanceParameters(instance).Recovery == false)
                throw new InvalidOperationException("Cannot start backup operation since the recovery option is disabled. In order to enable the recovery please set the RunInUnreliableYetFastModeThatIsNotSuitableForProduction configuration parameter value to false.");

            var backupOperation = new BackupOperation(filesystem, systemDatabase.Configuration.DataDirectory, backupDestinationDirectory, incrementalBackup, fileSystemDocument);
            Task.Factory.StartNew(backupOperation.Execute);
        }

        public void Restore(FilesystemRestoreRequest restoreRequest, Action<string> output)
        {
            new RestoreOperation(restoreRequest, configuration, output).Execute();
        }

		private void Output(string message)
		{
			Console.Write(message);
			Console.WriteLine();
		}
	}
}