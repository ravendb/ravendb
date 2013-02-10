using System;
using System.Collections.Generic;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Plugins;
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

        protected override void OnViewLoaded()
        {
            var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;

			if(databaseName == Constants.SystemDatabase)
			{
				var apiKeys = new ApiKeysSectionModel();
				Settings.Sections.Add(apiKeys);
				Settings.SelectedSection.Value = apiKeys;
				Settings.Sections.Add(new WindowsAuthSettingsSectionModel());

				return; 
			}

	        ApplicationModel.Current.Server.Value.DocumentStore
		        .AsyncDatabaseCommands
				.ForSystemDatabase()
		        .CreateRequest("/admin/databases/" + databaseName, "GET")
		        .ReadResponseJsonAsync()
		        .ContinueOnSuccessInTheUIThread(doc =>
		        {
			        if (doc == null)
				        return;

			        var databaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer()
						.Deserialize<DatabaseDocument>(new RavenJTokenReader(doc));
			        Settings.DatabaseDocument = databaseDocument;

			        var databaseSettingsSectionViewModel = new DatabaseSettingsSectionViewModel();
			        Settings.Sections.Add(databaseSettingsSectionViewModel);
			        Settings.SelectedSection.Value = databaseSettingsSectionViewModel;

					Settings.Sections.Add(new PeriodicBackupSettingsSectionModel());

			        string activeBundles;
			        databaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);

			        if (activeBundles != null)
			        {
						var bundles = activeBundles.Split(';').ToList();

				        if (bundles.Contains("Quotas"))
					        Settings.Sections.Add(new QuotaSettingsSectionModel());

				        if (bundles.Contains("Replication"))
					        Settings.Sections.Add(new ReplicationSettingsSectionModel());

						if(bundles.Contains("SqlReplication"))
							Settings.Sections.Add(new SqlReplicationSettingsSectionModel());

				        if (bundles.Contains("Versioning"))
					        Settings.Sections.Add(new VersioningSettingsSectionModel());

				        if (bundles.Contains("Authorization"))
					        Settings.Sections.Add(new AuthorizationSettingsSectionModel());
			        }

			        foreach (var settingsSectionModel in Settings.Sections)
			        {
				        settingsSectionModel.LoadFor(databaseDocument);
			        }

					var req = ApplicationModel.DatabaseCommands.ForSystemDatabase().CreateRequest("/plugins/status".NoCache(), "GET");

					req.ReadResponseJsonAsync().ContinueOnSuccessInTheUIThread(item =>
					{
						var plugins = ((RavenJObject)item).Deserialize<PluginsStatus>(new DocumentConvention());

						if (plugins == null || plugins.Plugins.Contains("Raven.Bundles.Authorization", StringComparer.InvariantCultureIgnoreCase) == false)
							return;

						var authSection = new AuthorizationSettingsSectionModel();
						Settings.Sections.Add(authSection);
						authSection.LoadFor(databaseDocument);
					});
		        });
        }

        public SettingsModel Settings { get; private set; }

        private ICommand _saveSettingsCommand;
        public ICommand SaveSettings { get { return _saveSettingsCommand ?? (_saveSettingsCommand = new SaveSettingsCommand(Settings)); } }
    }
}