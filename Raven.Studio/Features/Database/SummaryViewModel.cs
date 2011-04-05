namespace Raven.Studio.Features.Database
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Abstractions.Data;
    using Caliburn.Micro;
    using Client;
    using Collections;
    using Documents;
    using Framework;
    using Messages;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Raven.Database.Data;
    using Raven.Database.Indexing;
    using Raven.Database.Json;

    [ExportDatabaseScreen("Summary", Index = 10)]
    public class SummaryViewModel : RavenScreen, IDatabaseScreenMenuItem,
                                    IHandle<DocumentDeleted>,
                                    IHandle<StatisticsUpdated>
    {
        readonly IEventAggregator events;
        readonly IServer server;

        [ImportingConstructor]
        public SummaryViewModel(IServer server, IEventAggregator events)
            : base(events)
        {
            this.server = server;
            this.events = events;
            events.Subscribe(this);

            DisplayName = "Summary";

            server.CurrentDatabaseChanged += delegate
            {
                Collections = new BindableCollection<Collection>();
                RecentDocuments = new BindableCollection<DocumentViewModel>();

                CollectionsStatus = "Retrieving collections.";
                RecentDocumentsStatus = "Retrieving recent documents.";
                ShowCreateSampleData = false;
                IsGeneratingSampleData = false;

                NotifyOfPropertyChange(string.Empty);
            };
        }

        public string DatabaseName { get { return server.CurrentDatabase; } }

        public IServer Server { get { return server; } }

        public BindableCollection<DocumentViewModel> RecentDocuments { get; private set; }

        public BindableCollection<Collection> Collections { get; private set; }

        string collectionsStatus;
        public string CollectionsStatus
        {
            get { return collectionsStatus; }
            set
            {
                collectionsStatus = value;
                NotifyOfPropertyChange(() => CollectionsStatus);
            }
        }

        string recentDocumentsStatus;
        public string RecentDocumentsStatus
        {
            get { return recentDocumentsStatus; }
            set
            {
                recentDocumentsStatus = value;
                NotifyOfPropertyChange(() => RecentDocumentsStatus);
            }
        }

        public long LargestCollectionCount
        {
            get
            {
                return (Collections == null || !Collections.Any())
                        ? 0
                        : Collections.Max(x => x.Count);
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

        bool showCreateSampleData;

        public bool ShowCreateSampleData
        {
            get { return showCreateSampleData; }
            set { showCreateSampleData = value; NotifyOfPropertyChange(() => ShowCreateSampleData); }
        }

        public void BeginCreateSampleData()
        {
            var tasks = (IEnumerable<Task>)CreateSampleData().GetEnumerator();
            tasks.ExecuteInSequence(null);
        }

        IEnumerable<Task> CreateSampleData()
        {
            // this code assumes a small enough dataset, and doesn't do any sort
            // of paging or batching whatsoever.

            ShowCreateSampleData = false;
            IsGeneratingSampleData = true;

            WorkStarted("creating sample data");
            WorkStarted("creating sample indexes");
            using (var documentSession = Server.OpenSession())
            using (var sampleData = typeof(SummaryViewModel).Assembly.GetManifestResourceStream("Raven.Studio.SampleData.MvcMusicStore_Dump.json"))
            using (var streamReader = new StreamReader(sampleData))
            {
                var securityCheckId = "forceAuth_" + Guid.NewGuid();
                var putTask = documentSession.Advanced.AsyncDatabaseCommands
                    .PutAsync(securityCheckId, null, new JObject(), null);

                yield return putTask;

                if (putTask.Exception != null) yield break;

                yield return documentSession.Advanced.AsyncDatabaseCommands
                    .DeleteDocumentAsync(securityCheckId);

                var musicStoreData = (JObject)JToken.ReadFrom(new JsonTextReader(streamReader));
                foreach (var index in musicStoreData.Value<JArray>("Indexes"))
                {
                    var indexName = index.Value<string>("name");
                    var putDoc = documentSession.Advanced.AsyncDatabaseCommands
                        .PutIndexAsync(indexName,
                                        index.Value<JObject>("definition").JsonDeserialization<IndexDefinition>(),
                                        true);
                    yield return putDoc;
                }

                WorkCompleted("creating sample indexes");

                var batch = documentSession.Advanced.AsyncDatabaseCommands
                    .BatchAsync(
                        musicStoreData.Value<JArray>("Docs").OfType<JObject>().Select(
                        doc =>
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
                    );
                yield return batch;

                WorkCompleted("creating sample data");
                IsGeneratingSampleData = false;
                RecentDocumentsStatus = "Retrieving sample documents.";
                RetrieveSummary();
            }
        }

        bool isGeneratingSampleData;
        public bool IsGeneratingSampleData
        {
            get { return isGeneratingSampleData; }
            set { isGeneratingSampleData = value; NotifyOfPropertyChange(() => IsGeneratingSampleData); }
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
            RetrieveSummary();
        }

        void RetrieveSummary()
        {
            using (var session = server.OpenSession())
            {
                ExecuteCollectionQueryWithRetry(session, 5);

                WorkStarted("fetching recent documents");
                session.Advanced.AsyncDatabaseCommands
                    .GetDocumentsAsync(0, 12)
                    .ContinueWith(
                        x =>
                        {
                            WorkCompleted("fetching recent documents");
                            RecentDocuments = new BindableCollection<DocumentViewModel>(x.Result.Select(jdoc => new DocumentViewModel(jdoc)));
                            NotifyOfPropertyChange(() => RecentDocuments);

                            ShowCreateSampleData = !RecentDocuments.Any();

                            RecentDocumentsStatus = RecentDocuments.Any() ? string.Empty : "The database contains no documents.";

                        },
                        faulted =>
                        {
                            WorkCompleted("fetching recent documents");
                            NotifyError("Unable to retrieve recent documents from server.");
                        });
            }
        }

        void ExecuteCollectionQueryWithRetry(IAsyncDocumentSession session, int retry)
        {
            WorkStarted("fetching collections");
            session.Advanced.AsyncDatabaseCommands
                .GetCollectionsAsync(0, 25)
                .ContinueWith(task =>
                    {
                        if (task.Exception != null && retry > 0)
                        {
                            WorkCompleted("fetching collections");
                            TaskEx.Delay(50)
                                .ContinueWith(_ => ExecuteCollectionQueryWithRetry(session, retry - 1));
                            return;
                        }

                        task.ContinueWith(
                            x =>
                            {
                                WorkCompleted("fetching collections");
                                Collections = new BindableCollection<Collection>(x.Result);
                                NotifyOfPropertyChange(() => LargestCollectionCount);
                                NotifyOfPropertyChange(() => Collections);
                                CollectionsStatus = Collections.Any() ? string.Empty : "The database contains no collections.";
                            },
                            faulted =>
                            {
                                WorkCompleted("fetching collections");
                                const string error = "Unable to retrieve collections from server.";
                                NotifyError(error);
                                CollectionsStatus = error;
                                NotifyOfPropertyChange(() => LargestCollectionCount);
                                NotifyOfPropertyChange(() => Collections);

                            });
                    });
        }

        public void Handle(StatisticsUpdated message)
        {
            if (!message.HasDocumentCountChanged) return;

            RetrieveSummary();
        }
    }
}