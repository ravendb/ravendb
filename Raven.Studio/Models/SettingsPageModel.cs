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

            ApplicationModel.Current.Server.Value.DocumentStore
                .AsyncDatabaseCommands
                .ForDefaultDatabase()
                .CreateRequest("/admin/databases/" + databaseName, "GET")
                .ReadResponseJsonAsync()
                .ContinueOnSuccessInTheUIThread(doc =>
                {
                    if (doc == null)
                        return;

                    var databaseDocument = ApplicationModel.Current.Server.Value.DocumentStore.Conventions.CreateSerializer().Deserialize
                        <DatabaseDocument>(new RavenJTokenReader(doc));
                    Settings.DatabaseDocument = databaseDocument;

                    var databaseSettingsSectionViewModel = new DatabaseSettingsSectionViewModel();
                    Settings.Sections.Add(databaseSettingsSectionViewModel);
                    Settings.SelectedSection.Value = databaseSettingsSectionViewModel;

                    string activeBundles;
                    databaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);

					if (activeBundles != null)
					{
						if (activeBundles.Contains("Quotas"))
						{
							Settings.Sections.Add(new QuotaSettingsSectionModel());
						}

						if (activeBundles.Contains("Replication"))
						{
							Settings.Sections.Add(new ReplicationSettingsSectionModel());
						}

						if (activeBundles.Contains("Versioning"))
						{
							Settings.Sections.Add(new VersioningSettingsSectionModel());
						}
					}

	                foreach (var settingsSectionModel in Settings.Sections)
                    {
                        settingsSectionModel.LoadFor(databaseDocument);
                    }
                });
        }

        public SettingsModel Settings { get; private set; }

        private ICommand _saveBundlesCommand;
        public ICommand SaveBundles { get { return _saveBundlesCommand ?? (_saveBundlesCommand = new SaveBundlesCommand(Settings)); } }
    }
}