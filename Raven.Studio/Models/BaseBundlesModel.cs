using System.Collections.ObjectModel;
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
		public DatabaseDocument DatabaseDocument { get; set; }

		public BaseBundlesModel() 
		{
			ReplicationDestinations = new ObservableCollection<ReplicationDestination>();
			VersioningConfigurations = new ObservableCollection<VersioningConfiguration>();
		}

		public ReplicationDocument ReplicationData { get; set; }
		public ObservableCollection<VersioningConfiguration> VersioningConfigurations { get; set; }
		public string CurrentDatabase { get { return ApplicationModel.Database.Value.Name; } }
		public ReplicationDestination SelectedReplication { get; set; }
		public VersioningConfiguration SeletedVersioning { get; set; }
		public ObservableCollection<ReplicationDestination> ReplicationDestinations { get; set; }

		public virtual bool HasQuotas { get; set; }
		public virtual bool HasReplication { get; set; }
		public virtual bool HasVersioning { get; set; }
		public bool HasDefaultVersioning { get; set; }

		public virtual int MaxSize { get; set; }
		public virtual int WarnSize { get; set; }
		public virtual int MaxDocs { get; set; }
		public virtual int WarnDocs { get; set; }

		public ICommand DeleteReplication { get { return new DeleteReplicationCommand(this); } }
		public ICommand DeleteVersioning { get { return new DeleteVersioningCommand(this); } }
	}
}
