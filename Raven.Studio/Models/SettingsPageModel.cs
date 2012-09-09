using System;
using System.Collections.ObjectModel;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text.Implementation;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class SettingsPageModel : PageViewModel
    {
        private ICommand _saveBundlesCommand;

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

                    var activeBundles = string.Empty;
                    databaseDocument.Settings.TryGetValue("Raven/ActiveBundles", out activeBundles);
 
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

                    foreach (var settingsSectionModel in Settings.Sections)
                    {
                        settingsSectionModel.LoadFor(databaseDocument);
                    }
                });
        }

        public SettingsModel Settings { get; private set; }

        public ICommand SaveBundles { get { return _saveBundlesCommand ?? (_saveBundlesCommand = new SaveBundlesCommand(Settings)); } }
    }
}
