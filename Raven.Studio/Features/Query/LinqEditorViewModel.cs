namespace Raven.Studio.Features.Query
{
	using System;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Threading.Tasks;
	using Caliburn.Micro;
	using Client.Client;
	using Database;
	using Documents;
	using Framework;

	[Export]
	public class LinqEditorViewModel : Screen
	{
		readonly IServer server;
		string query;
		BindablePagedQuery<DocumentViewModel> queryResults;

		[ImportingConstructor]
		public LinqEditorViewModel(IServer server)
		{
			DisplayName = "Query";
			this.server = server;

			Query = "from doc in docs " + Environment.NewLine + "select doc";
		}

		public BindablePagedQuery<DocumentViewModel> QueryResults
		{
			get { return queryResults; }
			set
			{
				queryResults = value;
				NotifyOfPropertyChange(() => QueryResults);
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
			if (string.IsNullOrWhiteSpace(Query)) return;

			QueryResults = new BindablePagedQuery<DocumentViewModel>(BuildQuery);
			QueryResults.LoadPage();
		}

		Task<DocumentViewModel[]> BuildQuery(int start, int pageSize)
		{
			return server.OpenSession().Advanced.AsyncDatabaseCommands
				.LinearQueryAsync(Query, start, pageSize)
				.ContinueWith(x =>
				              	{
				              		QueryResults.GetTotalResults = () => x.Result.TotalResults;
				              		return x.Result.Results
				              			.Select(jobj => new DocumentViewModel(jobj.ToJsonDocument()))
				              			.ToArray();
				              	});
		}
	}
}