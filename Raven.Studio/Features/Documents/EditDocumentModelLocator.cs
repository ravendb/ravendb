using System;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{	
	public class EditDocumentModelLocator : ModelLocatorBase<EditableDocumentModel>
	{
		protected override void Load(DatabaseModel database, IAsyncDatabaseCommands asyncDatabaseCommands, Observable<EditableDocumentModel> observable)
		{
			var docId = ApplicationModel.Current.GetQueryParam("id");

			if (docId == null)
				return;

			asyncDatabaseCommands.GetAsync(docId)
				.ContinueOnSuccess(document =>
				{
					if (document == null)
					{
						ApplicationModel.Current.Navigate(new Uri("/DocumentNotFound?id=" + docId, UriKind.Relative));
						return;
					}
					observable.Value = new EditableDocumentModel(document, asyncDatabaseCommands);
				}
				)
				.Catch();
		}
	}
}