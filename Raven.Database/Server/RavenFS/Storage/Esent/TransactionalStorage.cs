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

using Microsoft.Isam.Esent.Interop;

using Raven.Abstractions.Exceptions;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
    public class TransactionalStorage : CriticalFinalizerObject, ITransactionalStorage
    {
		private readonly ThreadLocal<IStorageActionsAccessor> current = new ThreadLocal<IStorageActionsAccessor>();
		private readonly string database;
		private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
		private readonly string path;
		private readonly NameValueCollection settings;
		private readonly TableColumnsCache tableColumnsCache = new TableColumnsCache();
		private bool disposed;

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

        public TransactionalStorage(string path, RavenJObject settings)
        {
            this.settings = settings.ToNameValueCollection();
            this.path = path.ToFullPath();
            database = Path.Combine(this.path, "Data.ravenfs");

            new StorageConfigurator(this.settings).LimitSystemCache();

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

		public Guid Id { get; private set; }


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

		public void Initialize()
		{
			try
			{
				new StorageConfigurator(settings).ConfigureInstance(instance, path);

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
									new StorageConfigurator(settings).ConfigureInstance(recoverInstance.JetInstance, path);
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
	}
}