using System;
using System.IO;

namespace Raven.Munin.Tests
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
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_",  writeThrough: false);
            aggregateDictionary = new AggregateDictionary(persistentSource);

            persistentDictionaryOne = aggregateDictionary.Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));
            persistentDictionaryTwo = aggregateDictionary.Add(new PersistentDictionary(persistentSource, JTokenComparer.Instance));

            aggregateDictionary.Initialze();
        }
    }
}