using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class MultiDicInSingleFile : IDisposable
    {
        protected PersistentDictionaryAdapter persistentDictionaryOne;
        protected FileBasedPersistentSource persistentSource;
        protected PersistentDictionaryAdapter persistentDictionaryTwo;
        protected Database Database;

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
            Database.Commit();
            Database.BeginTransaction();
        }

        protected void Reopen()
        {
            persistentSource.Dispose();
            OpenDictionary();
        }

        protected void OpenDictionary()
        {
            persistentSource = new FileBasedPersistentSource(Path.GetTempPath(), "test_",  writeThrough: false);
            Database = new Database(persistentSource);

            persistentDictionaryOne = new PersistentDictionaryAdapter(Database.CurrentTransactionId, Database.Add(new Table(JTokenComparer.Instance)));
            persistentDictionaryTwo = new PersistentDictionaryAdapter(Database.CurrentTransactionId, Database.Add(new Table(JTokenComparer.Instance)));

            Database.Initialze();
            Database.BeginTransaction();
        }
    }
}