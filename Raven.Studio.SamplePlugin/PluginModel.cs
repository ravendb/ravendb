namespace Raven.Studio.SamplePlugin
{
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Plugins;
	using Plugins.Tasks;

	[ExportTask("Sample Plugin")]
	public class PluginModel : PropertyChangedBase
	{
		readonly IServer server;

		[ImportingConstructor]
		public PluginModel(IServer server)
		{
			this.server = server;
			Status = "Ready to do something!";
		}

		public void DeleteTemporaryDocument()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.DeleteDocumentAsync("someKnownId")
					.ContinueWith(_ =>
					{
						Status = _.IsFaulted
							? "Um, bad thing or something..."
							 : "Deleted!";
					});
			}
		}

		string status;

		public string Status
		{
			get { return status; }
			set { status = value; NotifyOfPropertyChange(() => Status); }
		}
	}
}