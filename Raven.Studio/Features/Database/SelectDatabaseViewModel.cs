namespace Raven.Studio.Features.Database
{
    using System.ComponentModel.Composition;
    using System.Windows;
    using Caliburn.Micro;
    using Framework;
    using Messages;
    using Plugins;
    using Raven.Client.Extensions;

    [Export]
    public class SelectDatabaseViewModel : RavenScreen
    {
        private string newDatabaseName;
        private Visibility showCreateDatabaseForm;

        [ImportingConstructor]
        public SelectDatabaseViewModel(IServer server, IEventAggregator events)
            : base(events)
        {
            DisplayName = "Home";
            Server = server;
            ShowCreateDatabaseForm = Visibility.Collapsed;
        }

        public IServer Server { get; private set; }

        public Visibility ShowCreateDatabaseForm
        {
            get { return showCreateDatabaseForm; }
            set
            {
                showCreateDatabaseForm = value;
                NotifyOfPropertyChange(() => ShowCreateDatabaseForm);
            }
        }

        public string NewDatabaseName
        {
            get { return newDatabaseName; }
            set
            {
                newDatabaseName = value;
                NotifyOfPropertyChange(() => NewDatabaseName);
            }
        }

        public void OpenSelectedDatabase()
        {
            SelectDatabase(Server.CurrentDatabase);
        }

        public void SelectDatabase(string database)
        {
            WorkStarted();

            Server.OpenDatabase(database, () =>
            {
                Events.Publish(new DisplayCurrentDatabaseRequested());
                WorkCompleted();
            });
        }

        public void BeginCreateNewDatabase()
        {
            ShowCreateDatabaseForm = Visibility.Visible;
        }

        public void CreateNewDatabase()
        {
            WorkStarted("creating database");
            Server.CreateDatabase(NewDatabaseName, ()=>
                {
                    WorkCompleted("creating database");
                    ShowCreateDatabaseForm = Visibility.Collapsed;
                    NewDatabaseName = string.Empty;
                });
        }
    }
}