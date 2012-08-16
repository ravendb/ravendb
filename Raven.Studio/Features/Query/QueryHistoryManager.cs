using System;
using System.Collections.Generic;
using System.IO.IsolatedStorage;
using System.Threading.Tasks;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Studio.Extensions;
using Raven.Studio.Models;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Query
{
    public class QueryHistoryManager
    {
        private const int MaxStoredHistoryEntries = 100;

        private readonly string databaseName;
        private readonly LinkedList<SavedQuery> recentQueries = new LinkedList<SavedQuery>();
        private readonly Dictionary<string, LinkedListNode<SavedQuery>> queriesByHash = new Dictionary<string, LinkedListNode<SavedQuery>>();
        private DateTime lastCleanupStarted = DateTime.MinValue;
        private Task loadHistoryTask;
        public event EventHandler<EventArgs> QueriesChanged;

        public QueryHistoryManager(string databaseName)
        {
            this.databaseName = databaseName;
            LoadPreviousQueriesFromDiskInBackground();
        }

        private void LoadPreviousQueriesFromDiskInBackground()
        {
            loadHistoryTask = Task.Factory.StartNew(
                () =>
                    {
                        using (
                            var storage =
                                IsolatedStorageFile.GetUserStoreForSite())
                        {
                            var path = GetFolderPath();
                            if (!storage.DirectoryExists(path))
                            {
                                storage.CreateDirectory(path);
                            }

                            var files = storage.GetFileNames(path + "/*.query");

                            var queries = (from fileName in files
                                           let content =
                                               storage.ReadEntireFile(path + "/" +
                                                                      fileName)
                                           let lastWriteTime =
                                               storage.GetLastWriteTime(path +
                                                                        "/" +
                                                                        fileName)
                                           let query =
                                               JsonConvert.DeserializeObject
                                               <SavedQuery>(content)
                                           orderby lastWriteTime descending
                                           select query)
                                .ToList();

                            return queries;
                        }
                    })
                .ContinueWith(
                    t =>
                        {
                            foreach (var query in t.Result)
                            {
                                LinkedListNode<SavedQuery> node;

                                if (!queriesByHash.TryGetValue(query.Hashcode, out node))
                                {
                                    node = new LinkedListNode<SavedQuery>(query);
                                    queriesByHash.Add(query.Hashcode, node);
                                    recentQueries.AddLast(node);
                                }
                                else
                                {
                                    // if the query was found, that must mean
                                    // the user has just updated it, so don't overwrite anything
                                }
                            }

                            OnQueriesChanged(EventArgs.Empty);
                        }, Schedulers.UIScheduler)
                .Catch();
        }

        public void StoreQuery(QueryState state)
        {
            var hash = state.GetHash();

            LinkedListNode<SavedQuery> node;

            if (!queriesByHash.TryGetValue(hash, out node))
            {
                node = new LinkedListNode<SavedQuery>(new SavedQuery(state.IndexName, state.Query));
                queriesByHash.Add(hash, node);
            }
            else
            {
                recentQueries.Remove(node);
            }

            node.Value.UpdateFrom(state);
            recentQueries.AddFirst(node);

            WriteToDisk(node.Value);
            OnQueriesChanged(EventArgs.Empty);
        }

        private void CleanupStoredHistory()
        {
			if (SystemTime.UtcNow - lastCleanupStarted > TimeSpan.FromMinutes(2))
            {
				lastCleanupStarted = SystemTime.UtcNow;

                Task.Factory.StartNew(
                    () =>
                        {

                            using (var storage = IsolatedStorageFile.GetUserStoreForSite())
                            {
                                var path = GetFolderPath();
                                if (!storage.DirectoryExists(path))
                                {
                                    return;
                                }

                                var fileNames = storage.GetFileNames(path + "/*.query");
                                if (fileNames.Length < MaxStoredHistoryEntries)
                                {
                                    return;
                                }

                                var oldestFiles = (from file in fileNames
                                                   let fullPath = path + "/" + file
                                                   let lastWriteTime =
                                                       storage.GetLastWriteTime(fullPath)
                                                   orderby lastWriteTime descending
                                                   select fullPath)
                                    .Skip(MaxStoredHistoryEntries)
                                    .ToList();

                                foreach (var filePath in oldestFiles)
                                {
                                    storage.DeleteFile(filePath);
                                }
                            }

                        })
                    .Catch();
            }
        }

        private string GetFolderPath()
        {
			if (databaseName == Constants.SystemDatabase)
				return "QueryHistory/System";
            return "QueryHistory/" + databaseName;
        }

        private void WriteToDisk(SavedQuery query)
        {
            try
            {
                using (var storage = IsolatedStorageFile.GetUserStoreForSite())
                {
                    var path = GetFolderPath();
                    if (!storage.DirectoryExists(path))
                    {
                        storage.CreateDirectory(path);
                    }

                    storage.WriteAllToFile(path + "/" + query.Hashcode + ".query", JsonConvert.SerializeObject(query, Formatting.None));
                }
            }
            catch (IsolatedStorageException ex)
            {
                ApplicationModel.Current.AddErrorNotification(ex, "Could not store query in query history");
            }

            CleanupStoredHistory();
        }

        public QueryState GetMostRecentStateForIndex(string indexName)
        {
            var savedQuery = recentQueries.FirstOrDefault(q => q.IndexName == indexName);

            if (savedQuery != null)
            {
                return ToQueryState(savedQuery);
            }
            else
            {
                return null;
            }
        }

        private static QueryState ToQueryState(SavedQuery savedQuery)
        {
            return new QueryState(savedQuery.IndexName, savedQuery.Query, savedQuery.SortOptions, savedQuery.IsSpatialQuery, savedQuery.Latitude, savedQuery.Longitude, savedQuery.Radius);
        }

        public IEnumerable<SavedQuery> RecentQueries
        {
            get { return recentQueries; }
        }

        protected void OnQueriesChanged(EventArgs e)
        {
            EventHandler<EventArgs> handler = QueriesChanged;
            if (handler != null) handler(this, e);
        }

        public bool IsHistoryLoaded
        {
            get { return loadHistoryTask != null && loadHistoryTask.IsCompleted; }
        }

        public Task WaitForHistoryAsync()
        {
            return loadHistoryTask;
        }

        public QueryState GetStateByHashCode(string hashCode)
        {
            LinkedListNode<SavedQuery> node;
            return queriesByHash.TryGetValue(hashCode, out node) ? ToQueryState(node.Value) : null;
        }

        public void PinQuery(SavedQuery query)
        {
            query.IsPinned = true;

            WriteToDisk(query);
            OnQueriesChanged(EventArgs.Empty);
        }

        public void UnPinQuery(SavedQuery query)
        {
            query.IsPinned = false;

            var node = queriesByHash[query.Hashcode];
            recentQueries.Remove(node);
            recentQueries.AddFirst(node);

            WriteToDisk(query);
            OnQueriesChanged(EventArgs.Empty);
        }

        public void ClearHistory()
        {
            var node = recentQueries.First;

            var filesToRemove = new List<string>();

            while (node != null)
            {
                var next = node.Next;

                if (!node.Value.IsPinned)
                {
                    queriesByHash.Remove(node.Value.Hashcode);
                    recentQueries.Remove(node);
                    filesToRemove.Add(node.Value.Hashcode);
                }

                node = next;
            }

            var currentTime = DateTimeOffset.Now;

            Task.Factory.StartNew(() =>
                                      {
                                          using (var storage = IsolatedStorageFile.GetUserStoreForSite())
                                          {
                                              var folder = GetFolderPath();
                                              foreach (var file in filesToRemove)
                                              {
                                                  var path = folder + "/" + file + ".query";
                                                  var lastAccessTime = storage.GetLastAccessTime(path);

                                                  if (lastAccessTime > currentTime)
                                                  {
                                                      continue;
                                                  }

                                                  storage.DeleteFile(path);
                                              }
                                          }
                                      })
                .Catch();

            OnQueriesChanged(EventArgs.Empty);
        }
    }
}
