using Raven.Abstractions.Extensions;
using Raven.Studio.Shell.MessageBox;

namespace Raven.Studio.Commands
{
	using System;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Documents;
	using Framework.Extensions;
	using Messages;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Plugins;

	public class SaveDocument
	{
		readonly IEventAggregator events;
		readonly IServer server;
		private readonly ShowMessageBox showMessageBox;

		[ImportingConstructor]
		public SaveDocument(IEventAggregator events, IServer server, ShowMessageBox showMessageBox)
		{
			this.events = events;
			this.server = server;
			this.showMessageBox = showMessageBox;
		}

		public void Execute(EditDocumentViewModel document)
		{
			if(!ValidateJson(document.JsonData) || !ValidateJson(document.JsonMetadata)) return;

			if (document.Id.StartsWith("Raven/"))
			{
				showMessageBox(
					"Are you sure that you want to edit a system document?",
					"Confirm Edit",
					MessageBoxOptions.OkCancel,
					box =>
						{
							if (box.WasSelected(MessageBoxOptions.Ok))
							{
								Delete(document);
							}
						});
				return;
			}
			Delete(document);
		}

		private void Delete(EditDocumentViewModel document)
		{
			document.PrepareForSave();

			var jdoc = document.JsonDocument;

			server.OpenSession().Advanced.AsyncDatabaseCommands
				.PutAsync(document.Id, jdoc.Etag, jdoc.DataAsJson, jdoc.Metadata)
				.ContinueOnSuccess(put =>
				{
					var inner = document.JsonDocument;
					inner.Key = put.Result.Key;
					inner.Metadata = inner.Metadata.FilterHeaders(isServerDocument: false);
					inner.Etag = put.Result.ETag;
					inner.LastModified = DateTime.Now;
					document.UpdateDocumentFromJsonDocument();

					events.Publish(new DocumentUpdated(document));
				});
		}

		bool ValidateJson(string json)
		{
			if(string.IsNullOrEmpty(json)) return true;

			try
			{
				JObject.Parse(json);
				return true;
			}
			catch (JsonReaderException exception)
			{
				events.Publish(new NotificationRaised(exception.Message, NotificationLevel.Error));
				return false;
			}
		}
	}
}