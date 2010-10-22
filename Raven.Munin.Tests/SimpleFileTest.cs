using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class SimpleFileTest : IDisposable
    {
        protected PersistentDictionaryAdapter PersistentDictionary;
        protected FileBasedPersistentSource PersistentSource;
        private readonly string tempPath;
        private AggregateDictionary aggregateDictionary;

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
            aggregateDictionary = new AggregateDictionary(PersistentSource);
            PersistentDictionary = new PersistentDictionaryAdapter(aggregateDictionary.CurrentTransactionId, aggregateDictionary.Add(new PersistentDictionary(JTokenComparer.Instance)));
            aggregateDictionary.Initialze();
            aggregateDictionary.BeginTransaction();
        }

        protected void SupressTx(Action action)
        {
            using (aggregateDictionary.SuppressTransaction())
                action();
        }

        protected void PerformIdleTasks()
        {
            aggregateDictionary.PerformIdleTasks();
        }

        protected void Rollback()
        {
            aggregateDictionary.Rollback();
            aggregateDictionary.BeginTransaction();
        }

        protected void Commit()
        {
            aggregateDictionary.Commit();
            aggregateDictionary.BeginTransaction();
        }

        public void Dispose()
        {
            PersistentSource.Dispose();
            PersistentSource.Delete();
        }
    }
}