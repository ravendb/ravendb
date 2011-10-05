// -----------------------------------------------------------------------
//  <copyright file="LogsModelLocator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class DocumentsModelLocator : ModelLocatorBase<DocumentsModel>
    {
        private IAsyncDatabaseCommands asyncDatabaseCommands;

        protected override void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<DocumentsModel> observable)
        {
            this.asyncDatabaseCommands = asyncDatabaseCommands;
            observable.Value = new DocumentsModel(GetFetchDocumentsMethod(), "/Documents", 25)
            {
                TotalPages = new Observable<long>(database.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / 25) 
            };
        }

        private Func<BindableCollection<ViewableDocument>, int, Task> GetFetchDocumentsMethod()
        {
            const int pageSize = 25;
            return (documents, currentPage) => asyncDatabaseCommands.GetDocumentsAsync(currentPage * pageSize, pageSize)
                .ContinueOnSuccess(docs => documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
        }
    }
}