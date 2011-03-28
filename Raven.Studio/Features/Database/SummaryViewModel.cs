using System.IO;
using System.Threading.Tasks;
using System.Windows;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using System.Linq;

namespace Raven.Studio.Features.Database
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Linq;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Collections;
	using Documents;
	using Framework;
	using Messages;

	public class SummaryViewModel : RavenScreen, IDatabaseScreenMenuItem,
		IHandle<DocumentDeleted>
	{
		readonly IServer server;
		readonly IEventAggregator events;

		public int Index { get { return 10; } }

		[ImportingConstructor]
		public SummaryViewModel(IServer server, IEventAggregator events)
			: base(events)
		{
			this.server = server;
			this.events = events;
			events.Subscribe(this);

			DisplayName = "Summary";

			server.CurrentDatabaseChanged += delegate { NotifyOfPropertyChange(string.Empty); };
		}

		public string DatabaseName
		{
			get { return server.CurrentDatabase; }
		}

		public IServer Server
		{
			get { return server; }
		}

		public BindableCollection<DocumentViewModel> RecentDocuments { get; private set; }

		public IEnumerable<Collection> Collections { get; private set; }

		public long LargestCollectionCount
		{
			get
			{
				return (Collections == null || !Collections.Any())
						? 0
						: Collections.Max(x => x.Count);
			}
		}

		public void CreateSampleData()
		{
			// this code assumes a small enough dataset, and doesn't do any sort
			// of paging or batching whatsoever.

			using(var sampleData= typeof(SummaryViewModel).Assembly.GetManifestResourceStream("Raven.Studio.SampleData.MvcMusicStore_Dump.json"))
			using(var streamReader = new StreamReader(sampleData))
			using(var documentSession = Server.OpenSession())
			{
				var musicStoreData = (JObject)JToken.ReadFrom(new JsonTextReader(streamReader));
				foreach (var index in musicStoreData.Value<JArray>("Indexes"))
				{
					var indexName = index.Value<string>("name");
					documentSession.Advanced.AsyncDatabaseCommands.PutIndexAsync(indexName,
						index.Value<JObject>("definition").JsonDeserialization<IndexDefinition>()
						, true)
						.ContinueOnSuccess(task => { });
				}

				documentSession.Advanced.AsyncDatabaseCommands.BatchAsync(
						musicStoreData.Value<JArray>("Docs").OfType<JObject>().Select(doc =>
						{
							var metadata = doc.Value<JObject>("@metadata");
							doc.Remove("@metadata");
							return new PutCommandData
							{
								Document = doc,
								Metadata = metadata,
								Key = metadata.Value<string>("@id"),
							};
						}).ToArray()
					).ContinueOnSuccess(task => { }); ;
			}
		}

		public void Handle(DocumentDeleted message)
		{
			RecentDocuments
				.Where(x => x.Id == message.DocumentId)
				.ToList()
				.Apply(x => RecentDocuments.Remove(x));

			//TODO: update collections
			//Collections
			//    .Where(x => x.Name == message.Document.CollectionType)
			//    .Apply(x => x.Count--);
		}

		public void NavigateToCollection(Collection collection)
		{
			events.Publish(new DatabaseScreenRequested(() =>
			{
				var vm = IoC.Get<CollectionsViewModel>();
				vm.ActiveCollection = collection;
				return vm;
			}));
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				WorkStarted("fetching collections");
				ExecuteCollectionQueryWithRetry(session, 5);

				WorkStarted("fetching recent documents");
				session.Advanced.AsyncDatabaseCommands
					.GetDocumentsAsync(0, 12)
					.ContinueOnSuccess(x =>
										{
											RecentDocuments = new BindableCollection<DocumentViewModel>(x.Result.Select(jdoc => new DocumentViewModel(jdoc)));
											NotifyOfPropertyChange(() => RecentDocuments);
											WorkCompleted("fetching recent documents");
										});
			}
		}

		private void ExecuteCollectionQueryWithRetry(IAsyncDocumentSession session, int retry)
		{
			session.Advanced.AsyncDatabaseCommands
				.GetCollectionsAsync(0, 25)
				.ContinueWith(task =>
				{
					if(task.Exception != null && retry > 0)
					{
						TaskEx.Delay(50)
							.ContinueWith(_ => ExecuteCollectionQueryWithRetry(session, retry - 1));
						return;
					}

					task.ContinueOnSuccess(x =>
					{
						Collections = x.Result;
						NotifyOfPropertyChange(() => LargestCollectionCount);
						NotifyOfPropertyChange(() => Collections);
						WorkCompleted("fetching collections");
					});
				});
		}
	}
}