using System;
using System.Reactive.Disposables;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Util;
using Raven.Studio.Features.Tasks;
using Raven.Studio.Infrastructure;
using System.Linq;
using System.Reactive.Linq;
using Raven.Studio.Extensions;
using VirtualCollection.VirtualCollection;

namespace Raven.Studio.Models
{
	public class DatabaseModel : Model, IDisposable
	{
		private readonly IAsyncDatabaseCommands asyncDatabaseCommands;
		private readonly string name;
	    private readonly DocumentStore documentStore;

	    private IObservable<DocumentChangeNotification> documentChanges;
	    private IObservable<IndexChangeNotification> indexChanges;

        private CompositeDisposable disposable = new CompositeDisposable();

	    public Observable<TaskModel> SelectedTask { get; set; }

		public DatabaseModel(string name, DocumentStore documentStore)
		{
			this.name = name;
		    this.documentStore = documentStore;

		    Tasks = new BindableCollection<TaskModel>(x => x.Name)
			{
				new ImportTask(),
				new ExportTask(),
				new StartBackupTask(),
				new IndexingTask()
			};

			SelectedTask = new Observable<TaskModel> {Value = Tasks.FirstOrDefault()};
			Statistics = new Observable<DatabaseStatistics>();

			asyncDatabaseCommands = name.Equals(Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase)
			                             	? documentStore.AsyncDatabaseCommands.ForDefaultDatabase()
			                             	: documentStore.AsyncDatabaseCommands.ForDatabase(name);
		}

		public BindableCollection<TaskModel> Tasks { get; private set; }

	    public IObservable<DocumentChangeNotification> DocumentChanges
	    {
            get
            {
                if (documentChanges == null)
                {
                    documentChanges = Changes()
                        .ForAllDocuments()
                        .Publish(); // use a single underlying subscription

                    var documentChangesSubscription =
                        ((IConnectableObservable<DocumentChangeNotification>) documentChanges).Connect();

                    disposable.Add(documentChangesSubscription);
                }

                return documentChanges;
            }
	    }

	    public IObservable<IndexChangeNotification> IndexChanges
	    {
            get
            {
                if (indexChanges == null)
                {
                    indexChanges = Changes()
                        .ForAllIndexes()
                        .Publish(); // use a single underlying subscription

                    var indexChangesSubscription = ((IConnectableObservable<IndexChangeNotification>)indexChanges).Connect();
                    disposable.Add(indexChangesSubscription);
                }
             
                return indexChanges;
            }
	    }

	    public IDatabaseChanges Changes()
        {
        	return name == Constants.SystemDatabase ? 
				documentStore.Changes() : 
				documentStore.Changes(name);
        }

		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get { return asyncDatabaseCommands; }
		}

		public string Name
		{
			get { return name; }
		}

		public Observable<DatabaseStatistics> Statistics { get; set; }

		public override Task TimerTickedAsync()
		{
			return asyncDatabaseCommands
				.GetStatisticsAsync()
				.ContinueOnSuccess(stats => Statistics.Value = stats);
		}

		private bool Equals(DatabaseModel other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.name, name);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof (DatabaseModel)) return false;
			return Equals((DatabaseModel) obj);
		}

		public override int GetHashCode()
		{
			return (name != null ? name.GetHashCode() : 0);
		}

	    public void Dispose()
	    {
	        disposable.Dispose();
	    }
	}
}