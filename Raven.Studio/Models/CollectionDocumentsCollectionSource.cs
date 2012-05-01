using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
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
                    Refresh();
                }
            }
        }

        protected override Task<int> GetCount()
        {
            return GetQueryResults(0, 1)
                .ContinueWith(t => t.Result.TotalResults,
                              TaskContinuationOptions.ExecuteSynchronously);
        }

        public override Task<IList<ViewableDocument>> GetPageAsync(int start, int pageSize, IList<SortDescription> sortDescriptions)
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
            if (string.IsNullOrEmpty(CollectionName))
            {
                return TaskEx.FromResult(new QueryResult());
            }

            return ApplicationModel.DatabaseCommands
                .QueryAsync("Raven/DocumentsByEntityName",
                            new IndexQuery {Start = start, PageSize = pageSize, Query = "Tag:" + CollectionName},
                            new string[] {});
        }
    }
}
