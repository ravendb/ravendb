using System;
using Newtonsoft.Json;
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
			var url = new UrlUtil();
			var docId = url.GetQueryParam("id");

			if (docId == null)
			{
				var projection = url.GetQueryParam("projection");
				if (projection == null) return;

				var document = JsonConvert.DeserializeObject<JsonDocument>(projection);
				observable.Value = new EditableDocumentModel(document, asyncDatabaseCommands);
				return;
			}
		
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