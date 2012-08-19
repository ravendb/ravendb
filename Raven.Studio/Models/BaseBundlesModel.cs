using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class BaseBundlesModel : ViewModel
	{
		public bool Creation { get; set; }
		public DatabaseDocument DatabaseDocument { get; set; }

		public BaseBundlesModel() 
		{
			Bundles = new ObservableCollection<string>();
			ReplicationDestinations = new ObservableCollection<ReplicationDestination>();
			VersioningConfigurations = new ObservableCollection<VersioningConfiguration>();
			SelectedBundle = new Observable<string>();

			VersioningConfigurations.CollectionChanged += (sender, args) => OnPropertyChanged(() => HasDefaultVersioning);
			SelectedBundle.PropertyChanged += (sender, args) =>
			{
				OnPropertyChanged(() => QuotasSelected);
				OnPropertyChanged(() => ReplicationSelected);
				OnPropertyChanged(() => VersioningSelected);
			};
		}

		public ReplicationDocument ReplicationData { get; set; }
		public ObservableCollection<VersioningConfiguration> VersioningConfigurations { get; set; }
		public ObservableCollection<VersioningConfiguration> OriginalVersioningConfigurations { get; set; }
		public string CurrentDatabase { get { return ApplicationModel.Database.Value.Name; } }
		public ReplicationDestination SelectedReplication { get; set; }
		public VersioningConfiguration SeletedVersioning { get; set; }
		public ObservableCollection<ReplicationDestination> ReplicationDestinations { get; set; }
		public ObservableCollection<string> Bundles { get; set; }
		public Observable<string> SelectedBundle { get; set; }

		public virtual bool HasQuotas { get; set; }
		public virtual bool HasReplication { get; set; }
		public virtual bool HasVersioning { get; set; }
		public bool HasDefaultVersioning
		{
			get { return VersioningConfigurations.Any(configuration => configuration.Id == "Raven/Versioning/DefaultConfiguration"); }
		}

		public bool QuotasSelected
		{
			get { return SelectedBundle.Value == "Quotas"; }
		}

		public bool ReplicationSelected
		{
			get { return SelectedBundle.Value == "Replication"; }
		}

		public bool VersioningSelected
		{
			get { return SelectedBundle.Value == "Versioning"; }
		}

		public virtual int MaxSize { get; set; }
		public virtual int WarnSize { get; set; }
		public virtual int MaxDocs { get; set; }
		public virtual int WarnDocs { get; set; }

		public ICommand DeleteReplication { get { return new DeleteReplicationCommand(this); } }
		public ICommand DeleteVersioning { get { return new DeleteVersioningCommand(this); } }
		public ICommand AddReplication { get { return new AddReplicationCommand(this); } }
		public ICommand AddVersioning { get { return new AddVersioningCommand(this); } }
		public ICommand AddDefaultVersioning { get { return new AddDefaultVersioningCommand(this); } }
	}
}
