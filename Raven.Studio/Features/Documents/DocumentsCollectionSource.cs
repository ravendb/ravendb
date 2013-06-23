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
    public class DocumentsCollectionSource : DocumentsVirtualCollectionSourceBase
    {
        public DocumentsCollectionSource()
        {
        }

        private void UpdateCount()
        {
            ApplicationModel.DatabaseCommands.GetStatisticsAsync().ContinueWith(t => SetCount((int)t.Result.CountOfDocuments)).Catch();
        }
        
        protected override Task<IList<ViewableDocument>> GetPageAsyncOverride(int start, int pageSize, IList<SortDescription> sortDescriptions)
        {
            if (!Count.HasValue)
            {
                UpdateCount();
            }

            return ApplicationModel.DatabaseCommands.GetDocumentsAsync(start, pageSize, MetadataOnly)
                .ContinueOnSuccess(t =>
                {
	                if (t == null)
		                return new List<ViewableDocument>();

                    var docs = (IList<ViewableDocument>)t.Select(x => new ViewableDocument(x)).ToArray();
                    return docs;
                })
                .Catch();
        }

        public async override Task<IAsyncEnumerator<JsonDocument>> StreamAsync(Reference<long> totalResults)
        {
            var statistics = await ApplicationModel.DatabaseCommands.GetStatisticsAsync();
            totalResults.Value = statistics.CountOfDocuments;

            var enumerator = await ApplicationModel.DatabaseCommands.StreamDocsAsync();

            return new ConvertingEnumerator<JsonDocument, RavenJObject>(enumerator, doc => doc.ToJsonDocument());
        }
    }
}