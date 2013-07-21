using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Silverlight.MissingFromSilverlight;
using Raven.Json.Linq;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
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


        public VersioningConfiguration SelectedVersioning { get; set; }
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
            SelectedVersioning = null;
        }

		public DatabaseDocument DatabaseDocument;

		public bool CadEditAllowedRevisions
		{
			get { return DatabaseDocument != null; }
		}

		private bool? allowChangedToRevistionsInternal;
		private Observable<bool> allowChangedToRevistions = new Observable<bool>();
		public Observable<bool> AllowChangedToRevistions
		{
			get
			{
				if (DatabaseDocument == null) //Not an admin
				{
					if (allowChangedToRevistionsInternal.HasValue)
					{
						allowChangedToRevistions.Value = allowChangedToRevistionsInternal.Value;
						return allowChangedToRevistions;
					}

					ApplicationModel.Current.Server.Value.SelectedDatabase.Value
							.AsyncDatabaseCommands
							.CreateRequest(string.Format("/debug/settings?key=Raven/Versioning/ChangesToRevisionsAllowed").NoCache(), "GET")
							.ReadResponseJsonAsync()
							.ContinueOnSuccessInTheUIThread(doc =>
							{
								if (doc == null)
								{
									allowChangedToRevistionsInternal = false;
									return;
								}
								bool value;
								if (bool.TryParse(doc.ToString(), out value) == false)
								{
									allowChangedToRevistionsInternal = false;
								}
								else
								{
									allowChangedToRevistionsInternal = value;
								}

								OnPropertyChanged(() => AllowChangedToRevistions);
							});
					allowChangedToRevistions.Value = false;
					return allowChangedToRevistions;
				}

				var changesToRevisionsAllowed = DatabaseDocument.Settings.ContainsKey("Raven/Versioning/ChangesToRevisionsAllowed");
				if (changesToRevisionsAllowed == false)
				{
					allowChangedToRevistions.Value = false;
					return allowChangedToRevistions;
				}
				bool result;
				if (bool.TryParse(DatabaseDocument.Settings["Raven/Versioning/ChangesToRevisionsAllowed"], out result) == false)
				{
					allowChangedToRevistions.Value = false;
					return allowChangedToRevistions;
				}

				allowChangedToRevistions.Value = result;
				return allowChangedToRevistions;
			}

			set
			{
				if(DatabaseDocument != null)
					DatabaseDocument.Settings["Raven/Versioning/ChangesToRevisionsAllowed"] = value.ToString();
			}
		}

		public override void LoadFor(DatabaseDocument document)
        {
	        DatabaseDocument = document;
            var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);
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