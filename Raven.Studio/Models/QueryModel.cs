using System.Windows.Input;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class QueryModel : Model
	{
		private readonly string indexName;
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		public const int PageSize = 25;

		public QueryModel(string indexName, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.indexName = indexName;
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			DocumentsResult = new Observable<DocumentsModel>();
		}

		public ICommand Execute { get { return new ExecuteQueryCommand(this, asyncDatabaseCommands); } }

		private string query;
		public string Query
		{
			get { return query; }
			set { query = value; OnPropertyChanged(); }
		}

		public string IndexName
		{
			get { return indexName; }
		}

		private string error;
		public string Error
		{
			get { return error; }
			set { error = value; OnPropertyChanged(); }
		}

		private int currentPage;
		public int CurrentPage
		{
			get { return currentPage; }
			set { currentPage = value; OnPropertyChanged(); }
		}

		public Observable<DocumentsModel> DocumentsResult { get; private set; }
	}
}