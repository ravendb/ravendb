using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;

using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;

namespace RavenFS.Tests.Storage
{
    public abstract class StorageAccessorTestBase : IDisposable
    {
        private readonly IList<string> pathsToDelete;

        private readonly IList<ITransactionalStorage> storages;

        public static IEnumerable<object[]> Storages
        {
            get
            {
                return new[]
				{
					new object[] {"voron"},
					new object[] {"esent"}
				};
            }
        }

        protected StorageAccessorTestBase()
        {
            pathsToDelete = new List<string>();
            storages = new List<ITransactionalStorage>();
        }

        protected string NewDataPath(string prefix = null)
        {
            if (prefix != null)
                prefix = prefix.Replace("<", "").Replace(">", "");

            var newDataDir = Path.GetFullPath(string.Format(@".\{0}-{1}-{2}\", DateTime.Now.ToString("yyyy-MM-dd,HH-mm-ss"), prefix ?? "TestDatabase", Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(newDataDir);
            pathsToDelete.Add(newDataDir);
            return newDataDir;
        }

        protected ITransactionalStorage NewTransactionalStorage(string requestedStorage, bool runInMemory = true, string path = null)
        {
            path = path ?? NewDataPath();

            var settings = new NameValueCollection
                           {
                               {"Raven/RunInMemory", runInMemory.ToString()}
                           };

            ITransactionalStorage storage;

            switch (requestedStorage)
            {
                case "esent":
                    storage = new Raven.Database.Server.RavenFS.Storage.Esent.TransactionalStorage(path, settings);
                    break;
                case "voron":
                    storage = new Raven.Database.Server.RavenFS.Storage.Voron.TransactionalStorage(path, settings);
                    break;
                default:
                    throw new NotSupportedException(string.Format("Given storage type ({0}) is not supported.", requestedStorage));
            }

            storages.Add(storage);
            storage.Initialize();

            return storage;
        }

        public void Dispose()
        {
            var exceptions = new List<Exception>();

            foreach (var storage in storages)
            {
                try
                {
                    storage.Dispose();
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            foreach (var path in pathsToDelete)
            {
                try
                {
                    IOExtensions.DeleteDirectory(path);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            if (exceptions.Count > 0)
                throw new AggregateException("There was an error during test disposal.", exceptions);
        }
    }
}