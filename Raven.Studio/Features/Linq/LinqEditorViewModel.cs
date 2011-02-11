namespace Raven.Studio.Features.Linq
{
	using System.Linq;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Database;
	using Plugin;
	using Raven.Client.Client;

	[Export]
	public class LinqEditorViewModel : Screen
	{
		readonly IServer server;
		string query;
		IList<DocumentViewModel> results;

		[ImportingConstructor]
		public LinqEditorViewModel(IServer server)
		{
			DisplayName = "Linq";
			this.server = server;
		}

		public IList<DocumentViewModel> Results
		{
			get { return results; }
			set
			{
				results = value;
				NotifyOfPropertyChange(() => Results);
			}
		}

		public string Query
		{
			get { return query; }
			set
			{
				query = value;
				NotifyOfPropertyChange(() => Query);
			}
		}

		public void Execute()
		{
			if (!string.IsNullOrWhiteSpace(Query))
			{
				server.OpenSession().Advanced.AsyncDatabaseCommands
					.LinearQueryAsync(Query,0,25)
					.ContinueWith(x=>
						{	
							var doc = IoC.Get<DocumentViewModel>();
							Results = x.Result.Results
								.Select(jobj => jobj.ToJsonDocument())
								.Select(doc.CloneUsing)
								.ToArray();
						});
			}
		}
	}
}