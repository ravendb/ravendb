using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Studio.Extensions;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class CreateSampleDataCommand : Command
	{
		private readonly Observable<DatabaseModel> database;
		private IObservable<Unit> databaseChanged;
		private Action<string> output;

		public CreateSampleDataCommand(Action<string> output)
		{
			this.output = output;
			database = ApplicationModel.Current.Server.Value.SelectedDatabase;

			databaseChanged = database
				.ObservePropertyChanged()
				.Select(e => Unit.Default);

			databaseChanged
				.SubscribeWeakly(this, (target, d) => target.HandleDatabaseChanged(target.database.Value));

			SubscribeToStatisticsChanged(database.Value);
		}

		private void HandleDatabaseChanged(DatabaseModel databaseModel)
		{
			RaiseCanExecuteChanged();

			SubscribeToStatisticsChanged(databaseModel);
		}

		private void SubscribeToStatisticsChanged(DatabaseModel databaseModel)
		{
			databaseModel.Statistics
				.ObservePropertyChanged()
				.TakeUntil(databaseChanged)
				.SubscribeWeakly(this, (target, e) => target.RaiseCanExecuteChanged());
		}

		public override bool CanExecute(object parameter)
		{
			return database.Value != null
				&& database.Value.Statistics.Value != null
				   && database.Value.Statistics.Value.CountOfDocuments == 0;
		}

		public override void Execute(object parameter)
		{
			CreateSampleData().ProcessTasks()
				.ContinueOnSuccessInTheUIThread(() => output("Sample Data Created") );
		}


		private IEnumerable<Task> CreateSampleData()
		{
			var commands = database.Value.AsyncDatabaseCommands;

			output("Createing Sample Data, Please wait...");

			// this code assumes a small enough dataset, and doesn't do any sort
			// of paging or batching whatsoever.

			using (var sampleData = typeof(HomeModel).Assembly.GetManifestResourceStream("Raven.Studio.Assets.EmbeddedData.MvcMusicStore_Dump.json"))
			using (var streamReader = new StreamReader(sampleData))
			{
				output("Reading documents");
				var musicStoreData = (RavenJObject)RavenJToken.ReadFrom(new JsonTextReader(streamReader));
				foreach (var index in musicStoreData.Value<RavenJArray>("Indexes"))
				{
					var indexName = index.Value<string>("name");
					var ravenJObject = index.Value<RavenJObject>("definition");
					output("Adding index " + indexName);
					var putDoc = commands
						.PutIndexAsync(indexName,
									   ravenJObject.JsonDeserialization<IndexDefinition>(),
									   true);
					yield return putDoc;
				}

				output("Storing documents");
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

			}
		}
	}
}
