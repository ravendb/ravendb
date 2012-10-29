using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

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
				if (ConfigurationManager.AppSettings.ContainsKey("Raven/ActiveBundles")
					&& ConfigurationManager.AppSettings["Raven/ActiveBundles"].Contains("PeriodicBackups"))
				{
					Settings.Sections.Add(new PeriodicBackupSettingsSectionModel());
				}

				Settings.Sections.Add(new WindowsAuthSettingsSectionModel());

				return; 
			}

	        ApplicationModel.Current.Server.Value.DocumentStore
		        .AsyncDatabaseCommands
		        .ForDefaultDatabase()
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

			        if (ConfigurationManager.AppSettings.ContainsKey("Raven/ActiveBundles")
			            && ConfigurationManager.AppSettings["Raven/ActiveBundles"].Contains("PeriodicBackups"))
			        {
				        Settings.Sections.Add(new PeriodicBackupSettingsSectionModel());
			        }

			        string activeBundles;
			        databaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);

			        if (activeBundles != null)
			        {
				        if (activeBundles.Contains("Quotas"))
					        Settings.Sections.Add(new QuotaSettingsSectionModel());

				        if (activeBundles.Contains("Replication"))
					        Settings.Sections.Add(new ReplicationSettingsSectionModel());

				        if (activeBundles.Contains("Versioning"))
					        Settings.Sections.Add(new VersioningSettingsSectionModel());

				        if (activeBundles.Contains("Authorization"))
					        Settings.Sections.Add(new AuthorizationSettingsSectionModel());
			        }

			        foreach (var settingsSectionModel in Settings.Sections)
			        {
				        settingsSectionModel.LoadFor(databaseDocument);
			        }
		        });
        }

        public SettingsModel Settings { get; private set; }

        private ICommand _saveSettingsCommand;
        public ICommand SaveSettings { get { return _saveSettingsCommand ?? (_saveSettingsCommand = new SaveSettingsCommand(Settings)); } }
    }
}