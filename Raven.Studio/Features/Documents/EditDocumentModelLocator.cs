using System;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{	
	public class EditDocumentModelLocator : ModelLocatorBase<EditableDocumentModel>
	{
		public static JsonDocument ProjectionDocument { get; set; }

		protected override void Load(IAsyncDatabaseCommands asyncDatabaseCommands, Observable<EditableDocumentModel> observable)
		{
			if(ApplicationModel.GetQueryParam("projection") == "true")
			{
				var state = ProjectionDocument;
				ProjectionDocument = null;
				observable.Value = new EditableDocumentModel(state, asyncDatabaseCommands);
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