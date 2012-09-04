using System.Collections.ObjectModel;
using System.Windows.Input;
using ActiproSoftware.Text.Implementation;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Bundles.Versioning.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using Raven.Client.Linq;

namespace Raven.Studio.Models
{
	public class SettingsModel : BaseSettingsModel
	{
		private string databaseName;

		public SettingsModel()
		{
			VersioningConfigurations.CollectionChanged += (sender, args) => OnPropertyChanged(() => HasDefaultVersioning);
			InitializeFromServer();
		}

		private void InitializeFromServer()
		{
			OriginalVersioningConfigurations = new ObservableCollection<VersioningConfiguration>();
			databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseName)
				.LoadAsync<ReplicationDocument>("Raven/Replication/Destinations")
				.ContinueOnSuccessInTheUIThread(document =>
				{
					if (document == null)
						return;
					ReplicationData = document;
					ReplicationDestinations.Clear();
					foreach (var replicationDestination in ReplicationData.Destinations)
					{
						ReplicationDestinations.Add(replicationDestination);
					}
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
						foreach (var versioningConfiguration in list)
						{
							OriginalVersioningConfigurations.Add(versioningConfiguration);							
						}
					});

			ApplicationModel.Current.Server.Value.DocumentStore
				.AsyncDatabaseCommands
				.ForDefaultDatabase()
				.CreateRequest("/admin/databases/" + ApplicationModel.Database.Value.Name, "GET")
				.ReadResponseJsonAsync()
				.ContinueOnSuccessInTheUIThread(doc =>
				{
					if (doc == null)
						return;
					DatabaseEditor = new EditorDocument
					{
						Text = doc.ToString(),
						Language = JsonLanguage
					};
					DatabaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer().Deserialize
						<DatabaseDocument>(new RavenJTokenReader(doc));
					OnPropertyChanged(() => HasQuotas);
					OnPropertyChanged(() => HasReplication);
					OnPropertyChanged(() => HasVersioning);
					OnPropertyChanged(() => MaxSize);
					OnPropertyChanged(() => WarnSize);
					OnPropertyChanged(() => MaxDocs);
					OnPropertyChanged(() => WarnDocs);
					OnPropertyChanged(() => DatabaseEditor);

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

					Settings.Add("Database Settings");
					SelectedSetting.Value = "Database Settings";

					if (HasQuotas)
						Settings.Add("Quotas");

					if (HasReplication)			
						Settings.Add("Replication");

					if (HasVersioning)
						Settings.Add("Versioning");

					OnPropertyChanged(() => Settings);
					OnPropertyChanged(() => SelectedSetting);
				});
		}

		public override bool HasQuotas
		{
			get { return ActiveBundles.Contains("Quotas"); }
		}

		private int maxSize;
		public override int MaxSize
		{
			get
			{
				if (maxSize == 0)
				{
					if (HasQuotas && DatabaseDocument.Settings.ContainsKey(Constants.SizeHardLimitInKB))
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
					if (HasQuotas && DatabaseDocument.Settings.ContainsKey(Constants.SizeSoftLimitInKB))
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
					if (HasQuotas && DatabaseDocument.Settings.ContainsKey(Constants.DocsHardLimit))
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
					if (HasQuotas && DatabaseDocument.Settings.ContainsKey(Constants.DocsSoftLimit))
						int.TryParse(DatabaseDocument.Settings[Constants.DocsSoftLimit], out warnDocs);
				}
				return warnDocs;
			}
			set { warnDocs = value; }
		}

		public override bool HasReplication
		{
			get { return ActiveBundles.Contains("Replication"); }
		}

		private string ActiveBundles
		{
			get
			{
				if (DatabaseDocument == null || DatabaseDocument.Settings == null)
					return string.Empty;

				string value;
				if (DatabaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out value))
					return value;
				return string.Empty;
			}
		}

		public override bool HasVersioning
		{
			get { return ActiveBundles.Contains("Versioning"); }
		}

		public ICommand SaveBundles { get { return new SaveBundlesCommand(this); } }
	}
}