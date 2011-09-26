// -----------------------------------------------------------------------
//  <copyright file="DeleteDocumentsCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Controls;
using Raven.Abstractions.Commands;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteDocumentsCommand : Command
	{
		public override void Execute(object parameter)
		{
			var listBox = GetList(parameter);
			if (listBox == null || listBox.SelectedItems.Count == 0)
				return;

			var documents = listBox.SelectedItems
				.Cast<ViewableDocument>()
				.ToList();
			var documentsIds = documents
				.Select(x => x.Id)
				.ToList();

			AskUser.ConfirmationAsync("Confirm Delete", documentsIds.Count > 1
										? string.Format("Are you sure you want to delete these {0} documents?", documentsIds.Count)
										: string.Format("Are you sure that you want to delete this document? ({0})", documentsIds.First()))
				.ContinueWhenTrue(() => DeleteDocuments(documentsIds))
				.ContinueOnSuccessInTheUIThread(() =>
									{
										var col = (BindableCollection<ViewableDocument>)listBox.DataContext;
										foreach (var document in documents)
										{
											col.Remove(document);
										}
									});
		}

		private void DeleteDocuments(IList<string> documentIds)
		{
			var deleteCommandDatas = documentIds.Select(id => new DeleteCommandData
			                                                  {
			                                                  	Key = id
			                                                  }).ToArray();
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.BatchAsync(deleteCommandDatas)
				.ContinueOnSuccess(() => ApplicationModel.Current.AddNotification(new Notification(documentIds.Count > 1
					? string.Format("{0} documents were deleted", documentIds.Count)
					: string.Format("Document {0} was deleted", documentIds.First()))));
		}

		private static ListBox GetList(object parameter)
		{
			if (parameter == null)
				return null;
			var menuItem = (MenuItem) parameter;
			var contextMenu = (ContextMenu) menuItem.Parent;
			if (contextMenu == null)
				return null;
			return (ListBox) contextMenu.Owner;
		}
	}
}