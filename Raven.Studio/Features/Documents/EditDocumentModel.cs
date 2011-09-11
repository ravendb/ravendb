using System.Threading.Tasks;
using Raven.Client.Connection.Async;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
	public class EditDocumentModel: Model
	{
		private readonly string docId;
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		public Observable<EditableDocument> Document { get; set; }
		public Observable<string> Notice { get; set; }

		public EditDocumentModel()
		{
			Notice = new Observable<string>();
			Document = new Observable<EditableDocument>();
			asyncDatabaseCommands = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands;
			docId = ApplicationModel.Current.GetQueryParam("id");
			RefreshAsync();
		}

		public Task RefreshAsync()
		{
			return asyncDatabaseCommands.GetAsync(docId)
				.ContinueOnSuccess(document => Document.Value = new EditableDocument(document))
				.Catch();
		}

		protected override Task TimerTickedAsync()
		{
			return asyncDatabaseCommands.GetAsync(docId)
				.ContinueOnSuccess(document =>
				{
					if (document == null)
					{
						Notice.Value = "Document " + docId + " was deleted on the server";
					}
					else if(document.Etag != Document.Value.Etag)
					{
						Notice.Value = "Document " + docId + " was changed on the server";
					}
				});
		}
	}
}