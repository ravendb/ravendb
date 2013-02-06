using System;
using System.Collections.Generic;
using System.ComponentModel;
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
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using System.Linq;
using Raven.Abstractions.Extensions;

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
            var query = new IndexQuery() {PageSize = pageSize, Start = start, SortedFields = new [] { new SortedField("-ConflictDetectedAt"), }};

            return ApplicationModel.DatabaseCommands.QueryAsync(ConflictsIndexName, query, new string[0])
                                   .ContinueWith(task =>
                                   {
                                       var documents =
                                           SerializationHelper.RavenJObjectsToJsonDocuments(task.Result.Results).Select(d => new ViewableDocument(d))
                                                              .ToArray();

                                       SetCount(Math.Max(task.Result.TotalResults - task.Result.SkippedResults,
                                                         documents.Length));

                                       return (IList<ViewableDocument>)documents;
                                   });
        }
    }
}
