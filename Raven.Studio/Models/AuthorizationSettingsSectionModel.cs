using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Bundles.Authorization.Model;
using Raven.Client.Connection.Async;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class AuthorizationSettingsSectionModel : SettingsSectionModel, IAutoCompleteSuggestionProvider
    {
        public AuthorizationSettingsSectionModel()
        {
            SectionName = "Authorization";
            OriginalAuthorizationRoles = new ObservableCollection<AuthorizationRole>();
            AuthorizationRoles = new ObservableCollection<AuthorizationRole>();
            OriginalAuthorizationUsers = new ObservableCollection<AuthorizationUser>();
            AuthorizationUsers = new ObservableCollection<AuthorizationUser>();

            AuthorizationRoles.Add(new AuthorizationRole
            {
                Id = "Authorization/Roles/Nurses",
                Permissions =
                {
                    new OperationPermission
                    {
                        Allow = true,
                        Operation = "Appointment/Schedule",
                        Tags = new List<string> {"Patient"},
                        Priority = 1
                    }
                }
            });

            AuthorizationRoles.Add(new AuthorizationRole
            {
                Id = "Authorization/Roles/Doctors",
                Permissions =
                {
                    new OperationPermission
                    {
                        Allow = true,
                        Operation = "Hospitalization/Authorize",
                        Tags = new List<string> {"Patient"}
                    }
                }
            });

            AuthorizationUsers.Add(new AuthorizationUser
            {
                Id = "Authorization/Users/DrHowser",
                Name = "Doogie Howser",
                Roles = {"Authorization/Roles/Doctors"},
                Permissions =
                {
                    new OperationPermission
                    {
                        Allow = true,
                        Operation = "Patient/View",
                        Tags = new List<string> {"Clinics/Kirya"}
                    },
                }
            });
        }

        public AuthorizationSettingsSectionModel(bool isCreation): this()
		{
			IsCreation = isCreation;
		}


        public AuthorizationRole SeletedRole { get; set; }
        public AuthorizationUser SeletedUser { get; set; }
		public bool IsCreation { get; set; }

        public ObservableCollection<AuthorizationRole> OriginalAuthorizationRoles { get; set; }
        public ObservableCollection<AuthorizationRole> AuthorizationRoles { get; set; }

        public ObservableCollection<AuthorizationUser> OriginalAuthorizationUsers { get; set; }
        public ObservableCollection<AuthorizationUser> AuthorizationUsers { get; set; }

        //private ICommand addAuthorizationRoleCommand;
        //private ICommand deleteAuthorizationRoleCommand;

        //public ICommand DeleteAuthorizationRole
        //{
        //    get { return deleteVersioningCommand ?? (deleteVersioningCommand = new ActionCommand(HandleDeleteVersioning)); }
        //}

        //public ICommand AddAuthorizationRole
        //{
        //    get
        //    {
        //        return addVersioningCommand ??
        //               (addVersioningCommand =
        //                new ActionCommand(() => VersioningConfigurations.Add(new VersioningConfiguration())));
        //    }
        //}

        //private void HandleDeleteAuthorizationRole(object parameter)
        //{
        //    var versioning = parameter as VersioningConfiguration;
        //    if (versioning == null)
        //        return;
        //    VersioningConfigurations.Remove(versioning);
        //    SeletedVersioning = null;
        //}

        public override void LoadFor(DatabaseDocument databaseDocument)
        {
            var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(databaseDocument.Id);
            session.Advanced.LoadStartingWithAsync<AuthorizationUser>("Authorization/Users").
		        ContinueOnSuccessInTheUIThread(data =>
		        {
                    AuthorizationUsers.Clear();
			        foreach (var authorizationUser in data)
			        {
                        AuthorizationUsers.Add(authorizationUser);
			        }
			        foreach (var authorizationUser in data)
			        {
                        AuthorizationUsers.Add(authorizationUser);
			        }
		        });
            session.Advanced.LoadStartingWithAsync<AuthorizationRole>("Authorization/Roles").
                ContinueOnSuccessInTheUIThread(data =>
                {
                    AuthorizationRoles.Clear();
                    foreach (var authorizationRole in data)
                    {
                        AuthorizationRoles.Add(authorizationRole);
                    }
                    foreach (var authorizationRole in data)
                    {
                        OriginalAuthorizationRoles.Add(authorizationRole);
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