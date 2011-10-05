using System;
using System.Threading.Tasks;
using System.Windows.Media;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
	public class CollectionModel : Model
	{
		private readonly IAsyncDatabaseCommands databaseCommands;
		Brush fill;
		public Brush Fill
		{
			get { return fill ?? (fill = TemplateColorProvider.Instance.ColorFrom(Name)); }
		}

		private string name;
		public string Name
		{
			get { return name; }
			set { name = value; OnPropertyChanged();}
		}

		private int count;
		public int Count
		{
			get { return count; }
			set { count = value; OnPropertyChanged();}
		}

		public Observable<DocumentsModel> Documents { get; set; }

		public CollectionModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
		    Documents = new Observable<DocumentsModel> {Value = new DocumentsModel(databaseCommands, GetFetchDocumentsMethod())};
		}

	    private Func<BindableCollection<ViewableDocument>, int, Task> GetFetchDocumentsMethod()
	    {
	        const int pageSize = 25;
	        return (docs, currentPage) => databaseCommands
                .QueryAsync("Raven/DocumentsByEntityName", new IndexQuery { Start = currentPage * pageSize, PageSize = pageSize, Query = "Tag:" + Name }, new string[] { })
                .ContinueOnSuccess(queryResult =>
                {
                    var documents = SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Results);
                    docs.Match(documents.Select(x => new ViewableDocument(x)).ToArray());
                });
	    }
	}
}