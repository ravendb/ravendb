//-----------------------------------------------------------------------
// <copyright file="MultiDicInSingleFile.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class MultiDicInSingleFile : IDisposable
    {
        protected Table tableOne;
        protected FileBasedPersistentSource persistentSource;
        protected Table tableTwo;
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

            tableOne = Database.Add(new Table("Test1"));
            tableTwo = Database.Add(new Table("Test2"));

            Database.Initialze();
            Database.BeginTransaction();
        }
    }
}