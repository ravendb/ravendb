namespace Raven.Studio.Commands
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Features.Database;
	using Framework;
	using Messages;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Plugin;

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

		public void Execute(DocumentViewModel document)
		{
			document.PrepareForSave();

			if(!ValidateJson(document.JsonData) || !ValidateJson(document.JsonMetadata)) return;

			server.OpenSession().Advanced.AsyncDatabaseCommands
				.PutAsync(document.Id, null, document.JsonDocument.DataAsJson,null)
				.ContinueOnSuccess(put => events.Publish(new DocumentUpdated(document)));
			
		}

		bool ValidateJson(string json)
		{
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