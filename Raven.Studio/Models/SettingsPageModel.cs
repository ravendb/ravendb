using System;
using System.Collections.Generic;
using System.Reactive;
using System.Reactive.Linq;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.Settings;
using Raven.Studio.Infrastructure;
using System.Linq;
using Raven.Client.Connection;

namespace Raven.Studio.Models
{
    public class SettingsPageModel : PageViewModel
    {
	    private bool firstLoad = true;

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
			if(firstLoad)
				RegisterToDatabaseChange();

		    firstLoad = false;

		    var databaseName = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name;
			Settings.Sections.Clear();
			OnPropertyChanged(() => CurrentDatabase);
		    if (databaseName == Constants.SystemDatabase)
		    {
			    var apiKeys = new ApiKeysSectionModel();
			    Settings.Sections.Add(apiKeys);
			    Settings.SelectedSection.Value = apiKeys;
			    Settings.Sections.Add(new WindowsAuthSettingsSectionModel());

			    return;
		    }

		    var debug = await ApplicationModel.DatabaseCommands.CreateRequest("/debug/config".NoCache(), "GET").ReadResponseJsonAsync();
		    var bundles = ApplicationModel.CreateSerializer()
		                                  .Deserialize<List<string>>(
			                                  new RavenJTokenReader(debug.SelectToken("ActiveBundles")));
		    var addedVersioning = false;
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

					//Bundles that need the database document
				    if (bundles.Contains("Quotas"))
					    Settings.Sections.Add(new QuotaSettingsSectionModel());

				    if (bundles.Contains("Versioning"))
				    {
					    AddModel(new VersioningSettingsSectionModel());
					    addedVersioning = true;
				    }

				    foreach (var settingsSectionModel in Settings.Sections)
				    {
					    settingsSectionModel.LoadFor(databaseDocument);
				    }
			    }
		    }

			//Bundles that don't need the database document
		    if (bundles.Contains("Replication"))
			    AddModel(new ReplicationSettingsSectionModel());

			 if (bundles.Contains("Versioning") && addedVersioning == false)
				 AddModel(new VersioningSettingsSectionModel());

		    if (bundles.Contains("SqlReplication"))
			    AddModel(new SqlReplicationSettingsSectionModel());

		    if (bundles.Contains("ScriptedIndexResults"))
			    AddModel(new ScriptedIndexSettingsSectionModel());

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
			    Settings.Sections.Add(new NoSettingsSectionModel());

			var url = new UrlParser(UrlUtil.Url);

			var id = url.GetQueryParam("id");
			if (string.IsNullOrWhiteSpace(id) == false)
			{
				switch (id)
				{
					case "scripted":
						if(Settings.Sections.Any(model => model is ScriptedIndexSettingsSectionModel))
							Settings.SelectedSection.Value = Settings.Sections.FirstOrDefault(model => model is ScriptedIndexSettingsSectionModel);
						break;
				}
			}
			else
				Settings.SelectedSection.Value = Settings.Sections[0];
	    }

	    public override bool CanLeavePage()
	    {
		    var unsavedSections = new List<string>();
		    foreach (var settingsSectionModel in Settings.Sections)
		    {
			    settingsSectionModel.CheckForChanges();
			    if(settingsSectionModel.HasUnsavedChanges)
					unsavedSections.Add(settingsSectionModel.SectionName);
		    }

		    if (unsavedSections.Count != 0)
			    return AskUser.Confirmation("Settings",
				    string.Format("There are unsaved changes in these sections: {0}. Are you sure you want to continue?"
					    , string.Join(", ", unsavedSections)));

		    return base.CanLeavePage();
	    }

	    private void RegisterToDatabaseChange()
	    {
		    var databaseChanged = Database.ObservePropertyChanged()
		                                  .Select(_ => Unit.Default)
		                                  .TakeUntil(Unloaded);

		    databaseChanged
			    .Subscribe(_ => OnViewLoaded());
	    }

	    private void AddModel(SettingsSectionModel model)
		{
			Settings.Sections.Add(model);
			model.LoadFor(null);
		}

	    public SettingsModel Settings { get; private set; }

        private ICommand saveSettingsCommand;
        public ICommand SaveSettings { get { return saveSettingsCommand ?? (saveSettingsCommand = new SaveSettingsCommand(Settings)); } }
    }
}