using System;
using System.IO;
using Raven.Storage.Managed.Impl;

namespace Raven.Tests.ManagedStorage.Impl
{
    public class SimpleFileTest : IDisposable
    {
        protected PersistentDictionary persistentDictionary;
        protected FileBasedPersistentSource persistentSource;
        private readonly string tempPath;
        private AggregateDictionary aggregateDictionary;

        public SimpleFileTest()
        {
            tempPath = Path.GetTempPath();
            OpenDictionary();
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(tempPath, "test_", writeThrough: true);
            aggregateDictionary = new AggregateDictionary(persistentSource);
            persistentDictionary = aggregateDictionary.Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));
            aggregateDictionary.Initialze();
        }

        protected void PerformIdleTasks()
        {
            aggregateDictionary.PerformIdleTasks();
        }

        protected void Commit(Guid txId)
        {
            aggregateDictionary.Commit(txId);
        }

        public void Dispose()
        {
            persistentSource.Dispose();
            persistentSource.Delete();
        }
    }
}