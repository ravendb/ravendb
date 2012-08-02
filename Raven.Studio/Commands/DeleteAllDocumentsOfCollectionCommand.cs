using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class DeleteAllDocumentsOfCollectionCommand : ListBoxCommand<CollectionModel>
	{
		public override void Execute(object parameter)
		{
			var collectionNames = SelectedItems
				.Select(x => x.Name)
				.ToList();

			AskUser.ConfirmationAsync("Confirm Delete", collectionNames.Count > 1
			                                            	? string.Format("Are you sure you want to delete all of the documents of these {0} collections?", collectionNames.Count)
			                                            	: string.Format("Are you sure that you want to delete all of the documents of this collection? ({0})", collectionNames.First()))
				.ContinueWhenTrue(() => DeleteDocuments(collectionNames));
		}

		private void DeleteDocuments(IList<string> collectionNames)
		{
			for (int i = 0; i < collectionNames.Count; i++)
			{
				var isLastItem = i == collectionNames.Count - 1;
				var name = collectionNames[i];
				DatabaseCommands.DeleteByIndexAsync("Raven/DocumentsByEntityName", new IndexQuery {Query = "Tag:" + name}, allowStale: true)
					.ContinueOnSuccessInTheUIThread(() =>
					                   	{
					                   		if (isLastItem == false) return;
					                   		var model = (Model) Context;
					                   		model.ForceTimerTicked();
					                   	});
			}
		}
	}
}
