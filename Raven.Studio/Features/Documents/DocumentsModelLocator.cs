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

		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<DocumentsModel> observable)
		{
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			var documents = new DocumentsModel(GetFetchDocumentsMethod);
			documents.Pager.SetTotalPages(new Observable<long>(DatabaseModel.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments / 25 + 1));
			observable.Value = documents;
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			return asyncDatabaseCommands.GetDocumentsAsync(documentsModel.Pager.Skip, documentsModel.Pager.PageSize)
				.ContinueOnSuccess(docs => documentsModel.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}
	}
}