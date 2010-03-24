using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage
{
    public class TransactionalStorage : CriticalFinalizerObject, IDisposable
    {
        [ThreadStatic] private static DocumentStorageActions current;
        private readonly string database;
        private readonly ReaderWriterLockSlim disposerLock = new ReaderWriterLockSlim();
        private readonly string path;
        private bool disposed;

        private IDictionary<string, JET_COLUMNID> documentsColumns;
        private IDictionary<string, JET_COLUMNID> filesColumns;
        private IDictionary<string, JET_COLUMNID> indexStatsColumns;
        private JET_INSTANCE instance;
        private IDictionary<string, JET_COLUMNID> tasksColumns;
        private IDictionary<string, JET_COLUMNID> mappedResultsColumns;

        public TransactionalStorage(string database)
        {
            this.database = database;
            path = database;
            if (Path.IsPathRooted(database) == false)
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
            this.database = Path.Combine(path, Path.GetFileName(database));
            Api.JetCreateInstance(out instance, database + Guid.NewGuid());
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

        public void Initialize()
        {
            try
            {
                ConfigureInstance(instance);
                Api.JetInit(ref instance);

                bool newDb = EnsureDatabaseIsCreatedAndAttachToDatabase();

                SetIdFromDb();

                InitColumDictionaries();

				if (newDb)
					AddDefaults();
            }
            catch (Exception e)
            {
                Dispose();
                throw new InvalidOperationException("Could not open transactional storage: " + database, e);
            }
        }

    	private void AddDefaults()
    	{
			JArray array;
			const string name = "Raven.Database.Defaults.default.json";
    		using(var defaultDocuments = typeof(TransactionalStorage).Assembly.GetManifestResourceStream(name))
    		{
    			array = JArray.Load(new JsonTextReader(new StreamReader(defaultDocuments)));
    		}

    		Batch(actions=>
    		{
				foreach (JObject document in array)
				{
					actions.AddDocument(
						document["DocId"].Value<string>(),
						document["Document"].Value<JObject>().ToString(),
						null, 
						document["Metadata"].Value<JObject>().ToString()
						);
				}

				actions.Commit();
    		});
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
                        documentsColumns = Api.GetColumnDictionary(session, documents);
                    using (var tasks = new Table(session, dbid, "tasks", OpenTableGrbit.None))
                        tasksColumns = Api.GetColumnDictionary(session, tasks);
                    using (var files = new Table(session, dbid, "files", OpenTableGrbit.None))
                        filesColumns = Api.GetColumnDictionary(session, files);
                    using (var indexStats = new Table(session, dbid, "indexes_stats", OpenTableGrbit.None))
                        indexStatsColumns = Api.GetColumnDictionary(session, indexStats);
                    using (var mappedResults = new Table(session, dbid, "mapped_results", OpenTableGrbit.None))
                        mappedResultsColumns = Api.GetColumnDictionary(session, mappedResults);
                }
                finally
                {
                    if (Equals(dbid, JET_DBID.Nil) == false)
                        Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                }
            }
        }

        private void ConfigureInstance(JET_INSTANCE jetInstance)
        {
            new InstanceParameters(jetInstance)
            {
                CircularLog = true,
                Recovery = true,
                CreatePathIfNotExist = true,
                TempDirectory = Path.Combine(path, "temp"),
                SystemDirectory = Path.Combine(path, "system"),
                LogFileDirectory = Path.Combine(path, "logs"),
                MaxVerPages = 8192
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
                        if (schemaVersion != SchemaCreator.SchemaVersion)
                            throw new InvalidOperationException("The version on disk (" + schemaVersion +
                                                                ") is different that the version supported by this library: " +
                                                                SchemaCreator.SchemaVersion + Environment.NewLine +
                                                                "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.");
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
                                    ConfigureInstance(recoverInstance.JetInstance);
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
        public void Batch(Action<DocumentStorageActions> action)
        {
            if (current != null)
            {
                try
                {
                    current.PushTx();
                    action(current);
                }
                finally
                {
                    current.PopTx();
                }
                return;
            }
            disposerLock.EnterReadLock();
            try
            {
                using (
                    var pht = new DocumentStorageActions(instance, database, documentsColumns, tasksColumns,
                                                         filesColumns, indexStatsColumns,mappedResultsColumns))
                {
                    current = pht;
                    action(pht);
                    if (pht.CommitCalled == false)
                        throw new InvalidOperationException("You forgot to call commit!");
                }
            }
            finally
            {
                disposerLock.ExitReadLock();
                current = null;
            }
        }
    }
}