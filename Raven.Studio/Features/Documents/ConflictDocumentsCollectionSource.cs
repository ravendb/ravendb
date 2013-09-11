using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System.Linq;

namespace Raven.Studio.Features.Documents
{
    public class ConflictDocumentsCollectionSource : VirtualCollectionSource<ViewableDocument>
    {
        private static readonly string ConflictsIndexName = "Raven/ConflictDocuments";

        public ConflictDocumentsCollectionSource()
        {
            
        }

        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            var query = new IndexQuery {PageSize = pageSize, Start = start, SortedFields = new [] { new SortedField("-ConflictDetectedAt"), }};

	        return ApplicationModel.DatabaseCommands.QueryAsync(ConflictsIndexName, query, new string[0])
	                               .ContinueWith(task =>
	                               {
		                               var documents =
			                               SerializationHelper.RavenJObjectsToJsonDocuments(task.Result.Results)
			                                                  .Select(d => new ViewableDocument(d))
			                                                  .ToArray();

		                               SetCount(Math.Max(task.Result.TotalResults - task.Result.SkippedResults,
		                                                 documents.Length));

		                               return (IList<ViewableDocument>) documents;
	                               });
        }
    }
}
