using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class CollectionDocumentsCollectionSource : VirtualCollectionSource<ViewableDocument>
    {
        private string _collectionName;
        private readonly object _lockObject = new object();

        public string CollectionName
        {
            get
            {
                lock (_lockObject)
                {
                    return _collectionName; 
                }
            }
            set
            {
                var needsUpdate = false;
                lock (_lockObject)
                {
                    needsUpdate = (_collectionName != value);
                    _collectionName = value;
                }

                if (needsUpdate)
                {
                    Refresh(RefreshMode.ClearStaleData);
                }
            }
        }

        protected override Task<int> GetCount()
        {
            return GetQueryResults(0, 1)
                .ContinueWith(t => t.Result.TotalResults,
                              TaskContinuationOptions.ExecuteSynchronously);
        }

        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            return GetQueryResults(start, pageSize)
                .ContinueWith(task =>
                                  {
                                      var documents =
                                          SerializationHelper.RavenJObjectsToJsonDocuments(task.Result.Results)
                                              .Select(x => new ViewableDocument(x))
                                              .ToArray();

                                      SetCount(task.Result.TotalResults);

                                      return (IList<ViewableDocument>) documents;
                                  });
        }

        private Task<QueryResult> GetQueryResults(int start, int pageSize)
        {
            string collectionName;
            lock (_lockObject)
            {
                collectionName = CollectionName;
            }

            if (string.IsNullOrEmpty(collectionName))
            {
                return TaskEx.FromResult(new QueryResult());
            }

            return ApplicationModel.DatabaseCommands
                .QueryAsync("Raven/DocumentsByEntityName",
                            new IndexQuery {Start = start, PageSize = pageSize, Query = "Tag:" + collectionName},
                            new string[] {})
                .Catch();
        }
    }
}
