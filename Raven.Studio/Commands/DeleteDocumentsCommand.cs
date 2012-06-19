// -----------------------------------------------------------------------
//  <copyright file="DeleteDocumentsCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteDocumentsCommand : VirtualItemSelectionCommand<ViewableDocument>
	{
	    public DeleteDocumentsCommand(ItemSelection<VirtualItem<ViewableDocument>> itemSelection) : base(itemSelection)
	    {
	    }

        protected override bool CanExecuteOverride(IList<ViewableDocument> items)
        {
            var document = items.FirstOrDefault();
            return document != null && document.Id != null;
        }

        protected override void ExecuteOverride(IList<ViewableDocument> realizedItems)
        {
            var documentsIds = realizedItems
				.Select(x => x.Id)
				.ToList();

			AskUser.ConfirmationAsync("Confirm Delete", documentsIds.Count > 1
			                                            	? string.Format("Are you sure you want to delete these {0} documents?", documentsIds.Count)
			                                            	: string.Format("Are you sure that you want to delete this document? ({0})", documentsIds.First()))
				.ContinueWhenTrueInTheUIThread(() =>
				                               	{
													ApplicationModel.Current.AddNotification(new Notification("Deleting documents..."));
				                               	})
				.ContinueWhenTrue(() => DeleteDocuments(documentsIds));
		}

		private void DeleteDocuments(IList<string> documentIds)
		{
			var deleteCommandDatas = documentIds
				.Select(id => new DeleteCommandData{Key = id})
				.Cast<ICommandData>()
				.ToArray();

			DatabaseCommands.BatchAsync(deleteCommandDatas)
				.ContinueOnSuccessInTheUIThread(() => DeleteDocumentSuccess(documentIds));
		}

		private void DeleteDocumentSuccess(IList<string> documentIds)
		{
			var notification = documentIds.Count > 1
								? string.Format("{0} documents were deleted", documentIds.Count)
								: string.Format("Document {0} was deleted", documentIds.First());
			ApplicationModel.Current.AddNotification(new Notification(notification));

            ClearSelection();
			PageView.UpdateAllFromServer();
		}
	}
}