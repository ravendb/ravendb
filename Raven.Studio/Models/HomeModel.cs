using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Extensions;

namespace Raven.Studio.Models
{
	public class HomeModel : PageViewModel
	{
		private DocumentsModel recentDocuments;

		public DocumentsModel RecentDocuments
		{
			get
			{
				if (recentDocuments == null)
				{
				    recentDocuments = (new DocumentsModel(new DocumentsCollectionSource())
				                                                      {
				                                                          Header = "Recent Documents",
                                                                          DocumentNavigatorFactory = (id, index) => DocumentNavigator.Create(id, index),
                                                                          Context = "AllDocuments",
				                                                      });
				}

			    return recentDocuments;
			}
		}

	    public HomeModel()
		{
			ModelUrl = "/home";

			ShowCreateSampleData = new Observable<bool>() { Value = RecentDocuments.Documents.Count == 0};

	        RecentDocuments.Documents.PropertyChanged +=
                delegate { ShowCreateSampleData.Value = RecentDocuments.Documents.Count == 0; };
		}

		public override void LoadModelParameters(string parameters)
		{
            RecentDocuments.TimerTickedAsync();
		}

		public override Task TimerTickedAsync()
		{
			return RecentDocuments.TimerTickedAsync();
		}

		public Observable<bool> ShowCreateSampleData { get; private set; }

		private bool isGeneratingSampleData;
		public bool IsGeneratingSampleData
		{
			get { return isGeneratingSampleData; }
			set { isGeneratingSampleData = value; OnPropertyChanged(() => IsGeneratingSampleData); }
		}

		#region Commands

		public ICommand CreateSampleData
		{
			get { return new CreateSampleDataCommand(this, Database); }
		}

		public class CreateSampleDataCommand : Command
		{
			private readonly HomeModel model;
            private readonly Observable<DatabaseModel> database;

			public CreateSampleDataCommand(HomeModel model, Observable<DatabaseModel> database)
			{
				this.model = model;
                this.database = database;

			    database.ObservePropertyChanged()
			        .SubscribeWeakly(this, (target, e) => RaiseCanExecuteChanged());
			}

            public override bool CanExecute(object parameter)
            {
                return database.Value != null
                       && database.Value.Statistics.Value.CountOfDocuments > 0;
            }

			public override void Execute(object parameter)
			{
				CreateSampleData().ProcessTasks()
					.ContinueOnSuccessInTheUIThread(() => model.ForceTimerTicked());
			}


			private IEnumerable<Task> CreateSampleData()
			{
			    var commands = database.Value.AsyncDatabaseCommands;

				// this code assumes a small enough dataset, and doesn't do any sort
				// of paging or batching whatsoever.

				model.ShowCreateSampleData.Value = false;
				model.IsGeneratingSampleData = true;

				using (var sampleData = typeof(HomeModel).Assembly.GetManifestResourceStream("Raven.Studio.Assets.EmbeddedData.MvcMusicStore_Dump.json"))
				using (var streamReader = new StreamReader(sampleData))
				{
					var musicStoreData = (RavenJObject)RavenJToken.ReadFrom(new JsonTextReader(streamReader));
					foreach (var index in musicStoreData.Value<RavenJArray>("Indexes"))
					{
						var indexName = index.Value<string>("name");
						var ravenJObject = index.Value<RavenJObject>("definition");
                        var putDoc = commands
							.PutIndexAsync(indexName,
										   ravenJObject.JsonDeserialization<IndexDefinition>(),
										   true);
						yield return putDoc;
					}

                    var batch = commands.BatchAsync(
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
