// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

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
		private string query;

		public ExecuteQueryCommand(QueryModel model, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.model = model;
			this.asyncDatabaseCommands = asyncDatabaseCommands;
		}

		public override bool CanExecute(object parameter)
		{
			query = parameter as string;
			return string.IsNullOrEmpty(query) == false;
		}

		public override void Execute(object parameter)
		{
			ApplicationModel.Current.AddNotification(new Notification("Executing query..."));

			var q = new IndexQuery {Start = 0, PageSize = QueryModel.PageSize, Query = query};
			asyncDatabaseCommands.QueryAsync(model.IndexName, q, null)
				.ContinueWith(result =>
				              {
				              	if (result.Exception != null)
				              	{
				              		model.Error = result.Exception.ExtractSingleInnerException().SimplifyError();
				              		return;
				              	}
				              	model.DocumentsResult.Value = new DocumentsModel(GetFetchDocumentsMethod, "/query", QueryModel.PageSize);
				              })
				.ContinueOnSuccess(() => ApplicationModel.Current.AddNotification(new Notification("Query executed.")));

		}

		private Task GetFetchDocumentsMethod(BindableCollection<ViewableDocument> documents, int currentPage)
		{
			var q = new IndexQuery { Start = model.CurrentPage * QueryModel.PageSize, PageSize = QueryModel.PageSize, Query = query };
			return asyncDatabaseCommands.QueryAsync(model.IndexName, q, null)
				.ContinueOnSuccess(result => documents.Match(result.Results.Select(obj => new ViewableDocument(obj.ToJsonDocument())).ToArray()));
		}
	}
}