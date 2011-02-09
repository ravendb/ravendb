//-----------------------------------------------------------------------
// <copyright file="SimpleFileTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;

namespace Raven.Munin.Tests
{
    public class SimpleFileTest : IDisposable
    {
        protected Table Table;
        protected FileBasedPersistentSource PersistentSource;
        private readonly string tempPath;
        private Database database;

        public SimpleFileTest()
        {
            tempPath = Path.GetTempPath();
        	var path = Path.Combine(tempPath, "test.ravendb");
			if (File.Exists(path))
				File.Delete(path);
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
            database = new Database(PersistentSource);
            Table = database.Add(new Table("Test"));
            database.Initialze();
            database.BeginTransaction();
        }

        protected void SupressTx(Action action)
        {
            using (database.SuppressTransaction())
                action();
        }

        protected void PerformIdleTasks()
        {
            database.PerformIdleTasks();
        }

        protected void Rollback()
        {
            database.Rollback();
            database.BeginTransaction();
        }

        protected void Commit()
        {
            database.Commit();
            database.BeginTransaction();
        }

        public void Dispose()
        {
            PersistentSource.Dispose();
            PersistentSource.Delete();
        }
    }
}