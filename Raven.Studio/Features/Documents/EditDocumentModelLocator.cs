using System;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class EditDocumentModelLocator
	{
		public Observable<EditableDocumentModel> Current
		{
			get
			{
				var observable = new Observable<EditableDocumentModel>();
				LoadDocument(observable);
				return observable;
			}
		}

		private void LoadDocument(Observable<EditableDocumentModel> observable)
		{
			var serverModel = ApplicationModel.Current.Server.Value;
			if (serverModel == null)
			{
				ApplicationModel.Current.Server.RegisterOnce(() => LoadDocument(observable));
				return;
			}

			var asyncDatabaseCommands = serverModel.SelectedDatabase.Value.AsyncDatabaseCommands;
			var docId = ApplicationModel.Current.GetQueryParam("id");

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