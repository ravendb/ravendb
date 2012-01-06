using System;
using System.Threading.Tasks;
using System.Windows.Media;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class CollectionModel : ViewModel
	{
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

		public CollectionModel()
		{
			Documents = new Observable<DocumentsModel> {Value = new DocumentsModel {CustomFetchingOfDocuments = GetFetchDocumentsMethod}};
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			if (string.IsNullOrWhiteSpace(Name)) return null;

			return DatabaseCommands
				.QueryAsync("Raven/DocumentsByEntityName", new IndexQuery { Start = documentsModel.Pager.Skip, PageSize = documentsModel.Pager.PageSize, Query = "Tag:" + Name }, new string[] { })
				.ContinueOnSuccess(queryResult =>
					{
						var documents = SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Results);
						documentsModel.Documents.Match(documents.Select(x => new ViewableDocument(x)).ToArray());
						Documents.Value.Pager.TotalResults.Value = queryResult.TotalResults;
					})
				.CatchIgnore<InvalidOperationException>(() => ApplicationModel.Current.AddNotification(new Notification("Unable to retrieve collections from server.", NotificationLevel.Error)));
		}
	}
}