using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class SimpleFileTest : IDisposable
    {
        protected PersistentDictionaryAdapter PersistentDictionary;
        protected FileBasedPersistentSource PersistentSource;
        private readonly string tempPath;
        private Database Database;

        public SimpleFileTest()
        {
            tempPath = Path.GetTempPath();
            OpenDictionary();
        }

        protected void Reopen()
        {
            PersistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            PersistentSource = new FileBasedPersistentSource(tempPath, "test", writeThrough: true);
            Database = new Database(PersistentSource);
            PersistentDictionary = new PersistentDictionaryAdapter(Database.CurrentTransactionId, Database.Add(new Table(JTokenComparer.Instance)));
            Database.Initialze();
            Database.BeginTransaction();
        }

        protected void SupressTx(Action action)
        {
            using (Database.SuppressTransaction())
                action();
        }

        protected void PerformIdleTasks()
        {
            Database.PerformIdleTasks();
        }

        protected void Rollback()
        {
            Database.Rollback();
            Database.BeginTransaction();
        }

        protected void Commit()
        {
            Database.Commit();
            Database.BeginTransaction();
        }

        public void Dispose()
        {
            PersistentSource.Dispose();
            PersistentSource.Delete();
        }
    }
}