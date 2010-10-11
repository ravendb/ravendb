using System;
using System.IO;
using Raven.Database;
using Raven.Storage.Managed.Impl;

namespace Raven.Tests.ManagedStorage.Impl
{
    public class MultiDicInSingleFile : IDisposable
    {
        protected PersistentDictionary persistentDictionaryOne;
        protected FileBasedPersistentSource persistentSource;
        protected PersistentDictionary persistentDictionaryTwo;
        protected AggregateDictionary aggregateDictionary;

        public MultiDicInSingleFile()
        {
            OpenDictionary();
        }

        #region IDisposable Members

        public void Dispose()
        {
            persistentSource.Dispose();
            persistentSource.Delete();
        }

        #endregion

        protected void Commit(Guid txId)
        {
            aggregateDictionary.Commit(txId);
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_", TransactionMode.Lazy);
            aggregateDictionary = new AggregateDictionary(persistentSource);

            persistentDictionaryOne = aggregateDictionary.Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));
            persistentDictionaryTwo = aggregateDictionary.Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));

            aggregateDictionary.Initialze();
        }
    }
}