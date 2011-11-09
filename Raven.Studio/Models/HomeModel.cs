using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class HomeModel : ViewModel
	{
		public Observable<DocumentsModel> RecentDocuments { get; private set; }

		public HomeModel()
		{
			ModelUrl = "/home";
			RecentDocuments = new Observable<DocumentsModel>();
			Initialize();
		}

		private void Initialize()
		{
			if (Database.Value == null)
			{
				Database.RegisterOnce(Initialize);
				return;
			}

			var documents = new DocumentsModel(GetFetchDocumentsMethod);
			documents.Pager.PageSize = 15;
			documents.Pager.SetTotalResults(new Observable<long>(Database.Value.Statistics, v => ((DatabaseStatistics)v).CountOfDocuments));
			RecentDocuments.Value = documents;
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documents)
		{
			return DatabaseCommands.GetDocumentsAsync(documents.Pager.Skip, documents.Pager.PageSize)
				.ContinueOnSuccess(docs => documents.Documents.Match(docs.Select(x => new ViewableDocument(x)).ToArray()));
		}

		private bool showCreateSampleData;
		public bool ShowCreateSampleData
		{
			get { return showCreateSampleData; }
			set { showCreateSampleData = value; OnPropertyChanged(); }
		}

		private bool isGeneratingSampleData;
		public bool IsGeneratingSampleData
		{
			get { return isGeneratingSampleData; }
			set { isGeneratingSampleData = value; OnPropertyChanged(); }
		}

		public BindableCollection<Notification> CreateSampleDataNotifications { get; set; }

		#region Commands

		public ICommand CreateSampleData
		{
			get { return new CreateSampleDataCommand(this, DatabaseCommands); }
		}

		public class CreateSampleDataCommand : Command
		{
			private readonly HomeModel model;
			private readonly IAsyncDatabaseCommands databaseCommands;

			public CreateSampleDataCommand(HomeModel model, IAsyncDatabaseCommands databaseCommands)
			{
				this.model = model;
				this.databaseCommands = databaseCommands;
			}

			public override void Execute(object parameter)
			{
				
			}

			private IEnumerable<Task> CreateSampleData()
			{
				// this code assumes a small enough dataset, and doesn't do any sort
				// of paging or batching whatsoever.

				model.ShowCreateSampleData = false;
				model.IsGeneratingSampleData = true;

				using (var sampleData = typeof(HomeModel).Assembly.GetManifestResourceStream("Raven.Studio.EmbeddedData.MvcMusicStore_Dump.json"))
				using (var streamReader = new StreamReader(sampleData))
				{
					var musicStoreData = (RavenJObject)RavenJToken.ReadFrom(new JsonTextReader(streamReader));
					foreach (var index in musicStoreData.Value<RavenJArray>("Indexes"))
					{
						var indexName = index.Value<string>("name");
						var ravenJObject = index.Value<RavenJObject>("definition");
						var putDoc = databaseCommands
							.PutIndexAsync(indexName,
							               ravenJObject.JsonDeserialization<IndexDefinition>(),
							               true);
						yield return putDoc;
					}

					var batch = databaseCommands.BatchAsync(
						musicStoreData.Value<RavenJArray>("Docs").OfType<RavenJObject>().Select(
							doc =>
								{
									var metadata = doc.Value<RavenJObject>("@metadata");
									doc.Remove("@metadata");
									return new PutCommandData
									       	{
									       		Document = doc,
									       		Metadata = metadata,
									       		Key = metadata.Value<string>("@id"),
									       	};
								}).ToArray()
						);
						
					yield return batch;

					model.IsGeneratingSampleData = false;
				}
			}
		}

#endregion

	}
}