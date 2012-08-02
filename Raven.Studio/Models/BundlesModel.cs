using System.Collections.Generic;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class BundlesModel : ViewModel
	{
		public List<string> AllTabs = new List<string>
		{
			"Selection",
			"Encryption",
			"Quotas",
			"Replication",
			"Versioning"
		};

		private RavenJToken databaseSettings; 

		public BundlesModel()
		{
		//	ReplicationData =
		//		ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession().LoadAsync<ReplicationDocument>();

			ApplicationModel.Current.Server.Value.DocumentStore
			.AsyncDatabaseCommands
			.ForDefaultDatabase()
			.GetAsync("Raven/Databases/" + ApplicationModel.Database.Value.Name)
			.ContinueOnSuccessInTheUIThread(doc =>
			{
				databaseSettings = doc.DataAsJson["Settings"];
				OnPropertyChanged(() => HasQuotas);
				OnPropertyChanged(() => HasReplication);
				OnPropertyChanged(() => HasVersioning);
				OnPropertyChanged(() => MaxSize);
				OnPropertyChanged(() => WarnSize);
				OnPropertyChanged(() => MaxDocs);
				OnPropertyChanged(() => WarnDocs);
			});
		}

		public ReplicationDocument ReplicationData { get; set; }
		public string CurrentDatabase { get { return ApplicationModel.Database.Value.Name; } }

		public bool HasQuotas
		{
			get
			{
				return databaseSettings != null && databaseSettings.SelectToken("Raven/ActiveBundles").ToString().Contains("Quotas");
			}
		}

		private int maxSize;
		public int MaxSize
		{
			get
			{
				if (maxSize == 0)
				{
					if (HasQuotas)
						int.TryParse(databaseSettings.SelectToken(Constants.SizeHardLimitInKB).ToString(), out maxSize);
					maxSize /= 1024;
				}
				return maxSize;
			}
			set { maxSize = value; }
		}

		private int warnSize;
		public int WarnSize
		{
			get
			{
				if (warnSize == 0)
				{
					if (HasQuotas)
						int.TryParse(databaseSettings.SelectToken(Constants.SizeSoftLimitInKB).ToString(), out warnSize);
					warnSize /= 1024;
				}
				return warnSize;
			}
			set { warnSize = value; }
		}

		private int maxDocs;
		public int MaxDocs
		{
			get
			{
				if (maxDocs == 0)
				{
					if (HasQuotas)
						int.TryParse(databaseSettings.SelectToken(Constants.DocsHardLimit).ToString(), out maxDocs);
				}
				return maxDocs;
			}
			set { maxDocs = value; }
		}

		private int warnDocs;
		public int WarnDocs
		{
			get
			{
				if (warnDocs == 0)
				{
					if (HasQuotas)
						int.TryParse(databaseSettings.SelectToken(Constants.DocsSoftLimit).ToString(), out warnDocs);
				}
				return warnDocs;
			}
			set { warnDocs = value; }
		}

		public bool HasReplication
		{
			get
			{
				return databaseSettings != null && databaseSettings.SelectToken("Raven/ActiveBundles").ToString().Contains("Replication");
			}
		}

		public bool HasVersioning
		{
			get
			{
				return databaseSettings != null && databaseSettings.SelectToken("Raven/ActiveBundles").ToString().Contains("Versioning");
			}
		}
	}
}
