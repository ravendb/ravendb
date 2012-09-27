using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Linq;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class VersioningSettingsSectionModel : SettingsSectionModel, IAutoCompleteSuggestionProvider
    {
        public VersioningSettingsSectionModel()
        {
            OriginalVersioningConfigurations = new ObservableCollection<VersioningConfiguration>();
            VersioningConfigurations = new ObservableCollection<VersioningConfiguration>();
            SectionName = "Versioning";
        }

		public VersioningSettingsSectionModel(bool isCreation) : this()
		{
			IsCreation = isCreation;
		}


        public VersioningConfiguration SeletedVersioning { get; set; }
		public bool IsCreation { get; set; }

        public ObservableCollection<VersioningConfiguration> OriginalVersioningConfigurations { get; set; }
        public ObservableCollection<VersioningConfiguration> VersioningConfigurations { get; set; }

        private ICommand addVersioningCommand;
        private ICommand deleteVersioningCommand;

        public ICommand DeleteVersioning
        {
            get { return deleteVersioningCommand ?? (deleteVersioningCommand = new ActionCommand(HandleDeleteVersioning)); }
        }

        public ICommand AddVersioning
        {
            get
            {
                return addVersioningCommand ??
                       (addVersioningCommand =
                        new ActionCommand(() => VersioningConfigurations.Add(new VersioningConfiguration())));
            }
        }

        private void HandleDeleteVersioning(object parameter)
        {
            var versioning = parameter as VersioningConfiguration;
            if (versioning == null)
                return;
            VersioningConfigurations.Remove(versioning);
            SeletedVersioning = null;
        }

        public override void LoadFor(DatabaseDocument databaseDocument)
        {
            var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseDocument.Id);
	        session.Advanced.LoadStartingWithAsync<VersioningConfiguration>("Raven/Versioning").
		        ContinueOnSuccessInTheUIThread(data =>
		        {
			        VersioningConfigurations.Clear();
			        foreach (var versioningConfiguration in data)
			        {
				        VersioningConfigurations.Add(versioningConfiguration);
			        }
			        foreach (var versioningConfiguration in data)
			        {
				        OriginalVersioningConfigurations.Add(versioningConfiguration);
			        }
		        });                
        }

		private const string CollectionsIndex = "Raven/DocumentsByEntityName";

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
				CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccess(collections => (IList<object>)collections.OrderByDescending(x => x.Count)
											.Where(x => x.Count > 0)
											.Select(col => col.Name).Cast<object>().ToList());
		}
    }
}
