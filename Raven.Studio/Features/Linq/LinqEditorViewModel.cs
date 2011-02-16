namespace Raven.Studio.Features.Linq
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
	using Plugin;

	[Export]
	public class LinqEditorViewModel : Screen
	{
		readonly IServer server;
		string query;
		BindablePagedQuery<EditDocumentViewModel> queryResults;

		[ImportingConstructor]
		public LinqEditorViewModel(IServer server)
		{
			DisplayName = "Linq";
			this.server = server;

			Query = "from doc in docs " + Environment.NewLine + "select doc";
		}

		public BindablePagedQuery<EditDocumentViewModel> QueryResults
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
			
			QueryResults = new BindablePagedQuery<EditDocumentViewModel>(BuildQuery);
			QueryResults.LoadPage();
		}

		Task<EditDocumentViewModel[]> BuildQuery(int start, int pageSize)
		{
			var vm = IoC.Get<EditDocumentViewModel>();
			return server.OpenSession().Advanced.AsyncDatabaseCommands
				.LinearQueryAsync(Query, start, pageSize)
				.ContinueWith(x =>
				              	{
									QueryResults.GetTotalResults = () => x.Result.TotalResults;
				              		return x.Result.Results
				              			.Select(jobj => jobj.ToJsonDocument())
				              			.Select(vm.CloneUsing)
				              			.ToArray();
				              	});
		}
	}
}