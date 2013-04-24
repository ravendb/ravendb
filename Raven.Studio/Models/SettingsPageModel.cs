using System.Collections.Generic;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
    public class SettingsPageModel : PageViewModel
    {
        public SettingsPageModel()
        {
            Settings = new SettingsModel();
        }

        public string CurrentDatabase
        {
            get { return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name; }
        }

	    protected override async void OnViewLoaded()
	    {
		    var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

		    if (databaseName == Constants.SystemDatabase)
		    {
			    var apiKeys = new ApiKeysSectionModel();
			    Settings.Sections.Add(apiKeys);
			    Settings.SelectedSection.Value = apiKeys;
			    Settings.Sections.Add(new WindowsAuthSettingsSectionModel());

			    return;
		    }

		    var debug = await ApplicationModel.DatabaseCommands.CreateRequest("/debug/config", "GET").ReadResponseJsonAsync();
		    var bundles = ApplicationModel.CreateSerializer()
		                                  .Deserialize<List<string>>(
			                                  new RavenJTokenReader(debug.SelectToken("ActiveBundles")));

		    if (ApplicationModel.Current.Server.Value.UserInfo.IsAdminGlobal)
		    {
			    var doc = await ApplicationModel.Current.Server.Value.DocumentStore
			                                    .AsyncDatabaseCommands
			                                    .ForSystemDatabase()
			                                    .CreateRequest("/admin/databases/" + databaseName, "GET")
			                                    .ReadResponseJsonAsync();

			    if (doc != null)
			    {
				    var databaseDocument =
					    ApplicationModel.CreateSerializer().Deserialize<DatabaseDocument>(new RavenJTokenReader(doc));
				    Settings.DatabaseDocument = databaseDocument;

				    var databaseSettingsSectionViewModel = new DatabaseSettingsSectionViewModel();
				    Settings.Sections.Add(databaseSettingsSectionViewModel);
				    Settings.SelectedSection.Value = databaseSettingsSectionViewModel;
				    Settings.Sections.Add(new PeriodicBackupSettingsSectionModel());

				    if (bundles.Contains("Quotas"))
					    Settings.Sections.Add(new QuotaSettingsSectionModel());

				    foreach (var settingsSectionModel in Settings.Sections)
				    {
					    settingsSectionModel.LoadFor(databaseDocument);
				    }
			    }
		    }

		    if (bundles.Contains("Replication"))
		    {
			    var repModel = new ReplicationSettingsSectionModel();
			    Settings.Sections.Add(repModel);
				repModel.LoadFor(null);
		    }

		    if (bundles.Contains("SqlReplication"))
		    {
			    var sqlModel = new SqlReplicationSettingsSectionModel();
			    Settings.Sections.Add(sqlModel);
				sqlModel.LoadFor(null);
		    }

		    if (bundles.Contains("Versioning"))
		    {
			    var verModel = new VersioningSettingsSectionModel();
			    Settings.Sections.Add(verModel);
				verModel.LoadFor(null);
		    }

		    if (bundles.Contains("Authorization"))
		    {
			    var triggers = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Statistics.Value.Triggers;
			    if (triggers.Any(info => info.Name.Contains("Authorization")))
			    {
				    var authModel = new AuthorizationSettingsSectionModel();
				    Settings.Sections.Add(authModel);
					authModel.LoadFor(null);
			    }
		    }

			if (Settings.Sections.Count == 0)
			{
				Settings.Sections.Add(new NoSettingsSectionModel());
			}
	    }

	    public SettingsModel Settings { get; private set; }

        private ICommand saveSettingsCommand;
        public ICommand SaveSettings { get { return saveSettingsCommand ?? (saveSettingsCommand = new SaveSettingsCommand(Settings)); } }
    }
}