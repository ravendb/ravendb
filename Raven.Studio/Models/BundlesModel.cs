using System.Collections.ObjectModel;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Client.Linq;

namespace Raven.Studio.Models
{
	public class BundlesModel : BaseBundlesModel
	{
		private string databaseName;

		public BundlesModel()
		{
			VersioningConfigurations.CollectionChanged += (sender, args) => OnPropertyChanged(() => HasDefaultVersioning);
			InitializeFromServer();
		}

		private void InitializeFromServer()
		{
			databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName)
				.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
				.ContinueOnSuccessInTheUIThread(document =>
				{
					if (document == null)
						return;
					ReplicationData = document;
					ReplicationDestinations = new ObservableCollection<ReplicationDestination>(ReplicationData.Destinations);
				});

			ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName)
				.Query<VersioningConfiguration>().ToListAsync().ContinueOnSuccessInTheUIThread(
					list =>
					{
						VersioningConfigurations.Clear();
						foreach (var versioningConfiguration in list)
						{
							VersioningConfigurations.Add(versioningConfiguration);
						}
						OriginalVersioningConfigurations = new ObservableCollection<VersioningConfiguration>(list);
					});

			ApplicationModel.Current.Server.Value.DocumentStore
				.AsyncDatabaseCommands
				.ForDefaultDatabase()
				.GetAsync("Raven/Databases/" + ApplicationModel.Database.Value.Name)
				.ContinueOnSuccessInTheUIThread(doc =>
				{
					DatabaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer().Deserialize
						<DatabaseDocument>(
							new RavenJTokenReader(doc.DataAsJson));
					OnPropertyChanged(() => HasQuotas);
					OnPropertyChanged(() => HasReplication);
					OnPropertyChanged(() => HasVersioning);
					OnPropertyChanged(() => MaxSize);
					OnPropertyChanged(() => WarnSize);
					OnPropertyChanged(() => MaxDocs);
					OnPropertyChanged(() => WarnDocs);

					if (HasVersioning)
					{
						ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName)
							.LoadAsync<object>("Raven/Versioning/DefaultConfiguration")
							.ContinueOnSuccessInTheUIThread(document =>
							{
								if (document != null)
								{
									VersioningConfigurations.Insert(0, document as VersioningConfiguration);
									OriginalVersioningConfigurations.Insert(0, document as VersioningConfiguration);
									OnPropertyChanged(() => HasDefaultVersioning);
								}
							});
					}

					if (HasQuotas)
					{
						Bundles.Add("Quotas");
						SelectedBundle.Value = "Quotas";
					}
					if (HasReplication)
					{
						Bundles.Add("Replication");
						if (SelectedBundle.Value == null)
							SelectedBundle.Value = "Replication";
					}
					if (HasVersioning)
					{
						Bundles.Add("Versioning");
						if (SelectedBundle.Value == null)
							SelectedBundle.Value = "Versioning";
					}
				});
		}

		public override bool HasQuotas
		{
			get{return DatabaseDocument != null && DatabaseDocument.Settings["Raven/ActiveBundles"].Contains("Quotas");}
		}

		private int maxSize;
		public override int MaxSize
		{
			get
			{
				if (maxSize == 0)
				{
					if (HasQuotas)
						int.TryParse(DatabaseDocument.Settings[Constants.SizeHardLimitInKB], out maxSize);
					maxSize /= 1024;
				}
				return maxSize;
			}
			set { maxSize = value; }
		}

		private int warnSize;
		public override int WarnSize
		{
			get
			{
				if (warnSize == 0)
				{
					if (HasQuotas)
						int.TryParse(DatabaseDocument.Settings[Constants.SizeSoftLimitInKB], out warnSize);
					warnSize /= 1024;
				}
				return warnSize;
			}
			set { warnSize = value; }
		}

		private int maxDocs;
		public override int MaxDocs
		{
			get
			{
				if (maxDocs == 0)
				{
					if (HasQuotas)
						int.TryParse(DatabaseDocument.Settings[Constants.DocsHardLimit], out maxDocs);
				}
				return maxDocs;
			}
			set { maxDocs = value; }
		}

		private int warnDocs;
		public override int WarnDocs
		{
			get
			{
				if (warnDocs == 0)
				{
					if (HasQuotas)
						int.TryParse(DatabaseDocument.Settings[Constants.DocsSoftLimit], out warnDocs);
				}
				return warnDocs;
			}
			set { warnDocs = value; }
		}

		public override bool HasReplication
		{
			get{return DatabaseDocument != null && DatabaseDocument.Settings["Raven/ActiveBundles"].Contains("Replication");}
		}

		public override bool HasVersioning
		{
			get{return DatabaseDocument != null && DatabaseDocument.Settings["Raven/ActiveBundles"].Contains("Versioning");}
		}

		public ICommand SaveBundles { get { return new SaveBundlesCommand(this); } }
	}
}