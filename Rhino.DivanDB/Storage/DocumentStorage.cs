using System;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Web;
using Microsoft.Isam.Esent.Interop;
using System.Diagnostics;

namespace Rhino.DivanDB.Storage
{
    public class DocumentStorage : CriticalFinalizerObject, IDisposable
    {
        private JET_INSTANCE instance;
        private readonly string database;
        private readonly string path;

        public Guid Id { get; private set; }

        public DocumentStorage(string database)
        {
            this.database = database;
            path = database;
            if (Path.IsPathRooted(database) == false)
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
            this.database = Path.Combine(path, Path.GetFileName(database));
            Api.JetCreateInstance(out instance, database + Guid.NewGuid());
        }

        public void Initialize()
        {
            ConfigureInstance(instance);
            try
            {
                Api.JetInit(ref instance);

                EnsureDatabaseIsCreatedAndAttachToDatabase();

                SetIdFromDb();
            }
            catch (Exception e)
            {
                Dispose();
                throw new InvalidOperationException("Could not open cache: " + database, e);
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
                            throw new InvalidOperationException("The version on disk (" + schemaVersion + ") is different that the version supported by this library: " + SchemaCreator.SchemaVersion + Environment.NewLine +
                                                                "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.");
                    }
                });
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." + Environment.NewLine +
                                                    "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", e);
            }
        }

        private void EnsureDatabaseIsCreatedAndAttachToDatabase()
        {
            using (var session = new Session(instance))
            {
                try
                {
                    Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                    return;
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
                        return;
                    }
                    if (e.Error != JET_err.FileNotFound)
                        throw;
                }

                new SchemaCreator(session).Create(database);
                Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Api.JetTerm2(instance, TermGrbit.Abrupt);
        }

        ~DocumentStorage()
        {
            try
            {
                Trace.WriteLine(
                    "Disposing esent resources from finalizer! You should call DocumentStorage.Dispose() instead!");
                Api.JetTerm(instance);
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
        [DebuggerHidden, DebuggerNonUserCode, DebuggerStepThrough, DebuggerStepperBoundary]
        public void Batch(Action<DocumentStorageActions> action)
        {
            using (var pht = new DocumentStorageActions(instance, database, Id))
            {
                action(pht);
            }
        }
    }
}