namespace Raven.Studio.Features.Indexes
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;
    using Client;
    using Database;
    using Framework;
    using Messages;
    using Plugin;
    using Raven.Database.Indexing;
    using System.Threading.Tasks;

    public class BrowseIndexesViewModel : Conductor<EditIndexViewModel>, IDatabaseScreenMenuItem,
                                          IHandle<IndexUpdated>
    {
        readonly IServer server;
        private readonly IEventAggregator events;
        IndexDefinition activeIndex;
        string filter;
        bool isBusy;

        public int Index { get { return 30; } }

        [ImportingConstructor]
        public BrowseIndexesViewModel(IServer server, IEventAggregator events)
        {
            DisplayName = "Indexes";

            this.server = server;
            this.events = events;
            events.Subscribe(this);

            Indexes = new BindablePagedQuery<IndexDefinition>((start, pageSize) =>
                                                                  {
                                                                      var session = server.OpenSession();

                                                                      return session.Advanced.AsyncDatabaseCommands
                                                                          .GetIndexesAsync(start, pageSize)
                                                                          .ContinueWith(x =>
                                                                                            {
                                                                                                session.Dispose();
                                                                                                return x;
                                                                                            }).Unwrap();

                                                                  });

        }

        protected override void OnActivate()
        {
            var session = server.OpenSession();

            BeginRefreshIndexes(session);
        }
        
        public void CreateNewIndex()
        {
            ActivateItem( new EditIndexViewModel(new IndexDefinition(){}, server,events));
        }

        private void BeginRefreshIndexes(IAsyncDocumentSession session)
        {
            IsBusy = true;

            session.Advanced.AsyncDatabaseCommands
                .GetStatisticsAsync()
                .ContinueOnSuccess(x => RefreshIndexes(x.Result.CountOfIndexes))
                .ContinueWith(task => session.Dispose());
        }

        public bool IsBusy
        {
            get { return isBusy; }
            set
            {
                isBusy = value;
                NotifyOfPropertyChange(() => IsBusy);
            }
        }

        public BindablePagedQuery<IndexDefinition> Indexes { get; private set; }

        public IndexDefinition ActiveIndex
        {
            get { return activeIndex; }
            set
            {
                activeIndex = value;
                if (activeIndex != null)
                    ActiveItem = new EditIndexViewModel(activeIndex, server,events);
                NotifyOfPropertyChange(() => ActiveIndex);
            }
        }

        public string Filter
        {
            get { return filter; }
            set
            {
                if (filter != value)
                {
                    filter = value;
                    NotifyOfPropertyChange(() => Filter);
                    Search(filter);
                }
            }
        }

        public void Handle(IndexUpdated message)
        {
             BeginRefreshIndexes(server.OpenSession());

             if(message.IsRemoved)
             {
                 ActiveItem = null;
             }
        }

        void RefreshIndexes(int totalIndexCount)
        {
            Indexes.GetTotalResults = () => totalIndexCount;
            Indexes.LoadPage();
            IsBusy = false;
        }

        public void Search(string text)
        {
            //text = text.Trim();
            //Items.Clear();

            //Items.AddRange(!string.IsNullOrEmpty(text) && text != WatermarkFilterString
            //                ? AllItems.Where(item => item.IndexOf(text, StringComparison.InvariantCultureIgnoreCase) >= 0)
            //                : AllItems);
        }
    }
}