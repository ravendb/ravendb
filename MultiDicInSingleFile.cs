using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class MultiDicInSingleFile : IDisposable
    {
        protected PersistentDictionaryAdapter persistentDictionaryOne;
        protected FileBasedPersistentSource persistentSource;
        protected PersistentDictionaryAdapter persistentDictionaryTwo;
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

        protected void Commit()
        {
            aggregateDictionary.Commit();
            aggregateDictionary.BeginTransaction();
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_",  writeThrough: false);
            aggregateDictionary = new AggregateDictionary(persistentSource);

            persistentDictionaryOne = new PersistentDictionaryAdapter(aggregateDictionary.CurrentTransactionId, aggregateDictionary.Add(new PersistentDictionary(JTokenComparer.Instance)));
            persistentDictionaryTwo = new PersistentDictionaryAdapter(aggregateDictionary.CurrentTransactionId, aggregateDictionary.Add(new PersistentDictionary(JTokenComparer.Instance)));

            aggregateDictionary.Initialze();
            aggregateDictionary.BeginTransaction();
        }
    }
}