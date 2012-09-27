using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Bundles.Authorization.Model;
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
        }

        public AuthorizationRole SeletedRole { get; set; }
        public AuthorizationUser SeletedUser { get; set; }
		public string NewRoleForUser { get; set; }
		public string SelectedRoleInUser { get; set; }

        public ObservableCollection<AuthorizationRole> OriginalAuthorizationRoles { get; set; }
        public ObservableCollection<AuthorizationRole> AuthorizationRoles { get; set; }

        public ObservableCollection<AuthorizationUser> OriginalAuthorizationUsers { get; set; }
        public ObservableCollection<AuthorizationUser> AuthorizationUsers { get; set; }

		private ICommand addAuthorizationRoleCommand;
		private ICommand deleteAuthorizationRoleCommand;
		private ICommand addAuthorizationUserCommand;
		private ICommand deleteAuthorizationUserCommand;
	    private ICommand deleteRoleFromUserCommand;
	    private ICommand addRoleToUserCommand;
	    private ICommand addPermissionToUserCommand;
	    private ICommand deletePermissionFromUserCommand;
		private ICommand addPermissionToRoleCommand;
		private ICommand deletePermissionFromRoleCommand;

	    public ICommand AddPermissionToUser
	    {
			get
			{
				return addPermissionToUserCommand ??
				       (addPermissionToUserCommand = new ActionCommand(() =>
				       {
					       SeletedUser.Permissions.Add(new OperationPermission());
						   AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
						   OnPropertyChanged(() => AuthorizationUsers);
				       }));
			}
	    }

		public ICommand DeletePermissionFromUser
		{
			get
			{
				return deletePermissionFromUserCommand ??
				       (deletePermissionFromUserCommand = new ActionCommand(HandleDeletePermissionFromUser));
			}
		}

		public ICommand AddPermissionToRole
		{
			get
			{
				return addPermissionToRoleCommand ??
					   (addPermissionToRoleCommand = new ActionCommand(() =>
					   {
						   SeletedRole.Permissions.Add(new OperationPermission());
						   AuthorizationRoles = new ObservableCollection<AuthorizationRole>(AuthorizationRoles);
						   OnPropertyChanged(() => AuthorizationRoles);
					   }));
			}
		}

		public ICommand DeletePermissionFromRole
		{
			get
			{
				return deletePermissionFromRoleCommand ??
					   (deletePermissionFromRoleCommand = new ActionCommand(HandleDeletePermissionFromRole));
			}
		}

	    public ICommand DeleteAuthorizationRole
		{
			get { return deleteAuthorizationRoleCommand ?? (deleteAuthorizationRoleCommand = new ActionCommand(HandleDeleteAuthorizationRole)); }
		}

		public ICommand AddAuthorizationRole
		{
			get
			{
				return addAuthorizationRoleCommand ?? (addAuthorizationRoleCommand =
						new ActionCommand(() => AuthorizationRoles.Add(new AuthorizationRole())));
			}
		}
	    public ICommand DeleteAuthorizationUser
	    {
		    get
		    {
			    return deleteAuthorizationUserCommand ??(deleteAuthorizationUserCommand = new ActionCommand(HandleDeleteAuthorizationUser));
		    }
	    }

	    public ICommand AddAuthorizationUser
		{
			get
			{
				return addAuthorizationUserCommand ?? (addAuthorizationUserCommand =
						new ActionCommand(() => AuthorizationUsers.Add(new AuthorizationUser())));
			}
		}

	    public ICommand DeleteRoleFromUserCommand
	    {
		    get { return deleteRoleFromUserCommand ?? (deleteRoleFromUserCommand = new ActionCommand(HandleDeleteRoleFromUser)); }
	    }

	    public ICommand AddRoleToUser
	    {
			get
			{
				return addRoleToUserCommand ?? (addRoleToUserCommand = new ActionCommand(() =>
				{
					if (string.IsNullOrWhiteSpace(NewRoleForUser))
						return;
					SeletedUser.Roles.Add(NewRoleForUser);
					NewRoleForUser = "";
					AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
					OnPropertyChanged(() => AuthorizationUsers);
				}));
			}
	    }
	    
		private void HandleDeleteRoleFromUser(object parameter)
		{
			var role = parameter as string;
			if (role == null)
				return;
			SeletedUser.Roles.Remove(role);
			AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
			OnPropertyChanged(() => AuthorizationUsers);
		}

		private void HandleDeletePermissionFromUser(object parameter)
		{
			var permission = parameter as OperationPermission;
			if(permission == null)
				return;

			SeletedUser.Permissions.Remove(permission);
			AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
			OnPropertyChanged(() => AuthorizationUsers);
		}

		private void HandleDeletePermissionFromRole(object parameter)
		{
			var permission = parameter as OperationPermission;
			if (permission == null)
				return;

			SeletedRole.Permissions.Remove(permission);
			AuthorizationRoles = new ObservableCollection<AuthorizationRole>(AuthorizationRoles);
			OnPropertyChanged(() => AuthorizationRoles);
		}

	    private void HandleDeleteAuthorizationRole(object parameter)
		{
			var role = parameter as AuthorizationRole;
			if (role == null)
				return;
			AuthorizationRoles.Remove(role);
			SeletedRole = null;
		}

		private void HandleDeleteAuthorizationUser(object parameter)
		{
			var user = parameter as AuthorizationUser;
			if (user == null)
				return;
			AuthorizationUsers.Remove(user);
			SeletedUser = null;
		}

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
						OriginalAuthorizationUsers.Add(authorizationUser);
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

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession().Advanced
				.LoadStartingWithAsync<AuthorizationRole>("Authorization/Roles")
				.ContinueOnSuccess(roles => (IList<object>)AuthorizationRoles.Select(role => role.Id).Cast<object>().ToList());
		}
    }
}