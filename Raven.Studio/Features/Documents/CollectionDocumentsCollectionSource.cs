using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class CollectionDocumentsCollectionSource : DocumentsVirtualCollectionSourceBase
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

        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
			if (CollectionName == "0")
			{
				return ApplicationModel.DatabaseCommands.StartsWithAsync("Raven/", null, start, 1024)
					.ContinueWith(t =>
					{
						var docs = (IList<ViewableDocument>)t.Result.Select(x => new ViewableDocument(x)).ToArray();
						SetCount(docs.Count);
						return docs;
					})
					.Catch();
			}
	        return GetQueryResults(start, pageSize)
		        .ContinueWith(task =>
		        {
			        var documents = SerializationHelper.RavenJObjectsToJsonDocuments(task.Result.Results)
			                                           .Select(x => new ViewableDocument(x))
			                                           .ToArray();

			        SetCount(task.Result.TotalResults - task.Result.SkippedResults);

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
                return TaskEx.FromResult(new QueryResult());

	        return ApplicationModel.DatabaseCommands
	                               .QueryAsync("Raven/DocumentsByEntityName",
	                                           new IndexQuery
	                                           {
		                                           Start = start,
		                                           PageSize = pageSize,
		                                           Query = "Tag:" + collectionName
	                                           },
	                                           new string[] {},
	                                           MetadataOnly)
	                               .Catch();
        }

        public async override Task<IAsyncEnumerator<JsonDocument>> StreamAsync(Reference<long> totalResults)
        {
            string collectionName;
            lock (_lockObject)
            {
                collectionName = CollectionName;
            }

            var reference = new Reference<QueryHeaderInformation>();

            var enumerator = await ApplicationModel.DatabaseCommands.StreamQueryAsync("Raven/DocumentsByEntityName",
                                                                                      new IndexQuery
                                                                                      {
                                                                                          Query = "Tag:" + collectionName
                                                                                      },
                                                                                      reference);

            totalResults.Value = reference.Value.TotalResults;

            return new ConvertingEnumerator<JsonDocument, RavenJObject>(enumerator, doc => doc.ToJsonDocument());
        }
    }
}
