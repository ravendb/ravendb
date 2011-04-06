namespace Raven.Studio.Commands
{
	using System;
	using Database.Data;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
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

		[ImportingConstructor]
		public SaveDocument(IEventAggregator events, IServer server)
		{
			this.events = events;
			this.server = server;
		}

		public void Execute(EditDocumentViewModel document)
		{
			if(!ValidateJson(document.JsonData) || !ValidateJson(document.JsonMetadata)) return;

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