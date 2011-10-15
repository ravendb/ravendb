// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;
using Raven.Client.Extensions;

namespace Raven.Studio.Features.Query
{
	public class ExecuteQueryCommand : Command
	{
		private readonly QueryModel model;
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;

		public ExecuteQueryCommand(QueryModel model, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.model = model;
			this.asyncDatabaseCommands = asyncDatabaseCommands;
		}

		public override bool CanExecute(object parameter)
		{
			return true;
		}

		public override void Execute(object parameter)
		{
			model.DocumentsResult.Value = new DocumentsModel(GetFetchDocumentsMethod, "/query", QueryModel.PageSize);
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documentsModel,int currentPage)
		{
			ApplicationModel.Current.AddNotification(new Notification("Executing query..."));
			var q = new IndexQuery { Start = model.CurrentPage * QueryModel.PageSize, PageSize = QueryModel.PageSize, Query = model.Query.Value };
			return asyncDatabaseCommands.QueryAsync(model.IndexName, q, null)
				.ContinueWith(task =>
				{
					if (task.Exception != null)
					{
						model.Error = task.Exception.ExtractSingleInnerException().SimplifyError();
						return;
					}
					var qr = task.Result;
					var viewableDocuments = qr.Results.Select(obj => new ViewableDocument(obj.ToJsonDocument())).ToArray();
					documentsModel.Documents.Match(viewableDocuments);
					documentsModel.TotalPages.Value = qr.TotalResults/QueryModel.PageSize;
				})
				.ContinueOnSuccess(() => ApplicationModel.Current.AddNotification(new Notification("Query executed.")))
				.Catch();
		}
	}
}