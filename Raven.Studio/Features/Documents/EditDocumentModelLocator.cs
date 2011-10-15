using System;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{	
	public class EditDocumentModelLocator : ModelLocatorBase<EditableDocumentModel>
	{
		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<EditableDocumentModel> observable)
		{
			if(ApplicationModel.GetQueryParam("projection") == "true")
			{
				var state = ApplicationModel.Current.State;
				ApplicationModel.Current.State = null;
				observable.Value = new EditableDocumentModel((JsonDocument)state, asyncDatabaseCommands);
			}

			var docId = ApplicationModel.GetQueryParam("id");

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