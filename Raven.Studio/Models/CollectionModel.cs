using System;
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

		public BindableCollection<ViewableDocument> Documents { get; set; }

		public CollectionModel(IAsyncDatabaseCommands databaseCommands)
		{
			this.databaseCommands = databaseCommands;
			Documents = new BindableCollection<ViewableDocument>(new PrimaryKeyComparer<ViewableDocument>(doc=>doc.Id));
		}

		protected override System.Threading.Tasks.Task TimerTickedAsync()
		{
			var query = new IndexQuery { Start = GetSkipCount(), PageSize = 25, Query = "Tag:" + Name };
			return databaseCommands
				.QueryAsync("Raven/DocumentsByEntityName", query, new string[] {})
				.ContinueOnSuccess(queryResult =>
				{
					var documents = SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Results);
					Documents.Match(documents.Select(x=>new ViewableDocument(x)).ToArray());
				});

		}
	}
}