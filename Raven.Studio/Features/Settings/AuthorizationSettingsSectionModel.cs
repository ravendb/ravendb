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
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
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

		public override void CheckForChanges()
		{
			if (HasUnsavedChanges)
				return;

			if (AuthorizationRoles.Count != OriginalAuthorizationRoles.Count)
			{
				HasUnsavedChanges = true;
				return;
			}

			if (AuthorizationUsers.Count != OriginalAuthorizationUsers.Count)
			{
				HasUnsavedChanges = true;
				return;
			}

			foreach (var authorizationRole in AuthorizationRoles)
			{
				if (authorizationRole.DeepEquals(OriginalAuthorizationRoles.FirstOrDefault(role => role.Id == authorizationRole.Id)) == false)
				{
					HasUnsavedChanges = true;
					return;
				}
			}
		}

		public override void MarkAsSaved()
		{
			HasUnsavedChanges = false;
			OriginalAuthorizationRoles = AuthorizationRoles;
			OriginalAuthorizationUsers = AuthorizationUsers;
		}

		public AuthorizationRole SelectedRole { get; set; }
		public AuthorizationUser SelectedUser { get; set; }
		public string NewRoleForUser { get; set; }
		public string SelectedRoleInUser { get; set; }
		public string SearchUsers { get; set; }

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
		private ICommand searchCommand;

		public ICommand Search { get { return searchCommand ?? (searchCommand = new ActionCommand(HandleSearchUsers)); } }

		public ICommand AddPermissionToUser
		{
			get
			{
				return addPermissionToUserCommand ??
					   (addPermissionToUserCommand = new ActionCommand(() =>
					   {
						   SelectedUser.Permissions.Add(new OperationPermission());
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
						   SelectedRole.Permissions.Add(new OperationPermission());
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
				return deleteAuthorizationUserCommand ?? (deleteAuthorizationUserCommand = new ActionCommand(HandleDeleteAuthorizationUser));
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
					SelectedUser.Roles.Add(NewRoleForUser);
					NewRoleForUser = "";
					AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
					OnPropertyChanged(() => AuthorizationUsers);
				}));
			}
		}

		private void HandleSearchUsers()
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);
			session.Advanced.LoadStartingWithAsync<AuthorizationUser>("Authorization/Users/" + SearchUsers).
				ContinueOnSuccessInTheUIThread(data =>
				{
					AuthorizationUsers.Clear();
					OriginalAuthorizationUsers.Clear();
					foreach (var authorizationUser in data)
					{
						AuthorizationUsers.Add(authorizationUser);
					}
					foreach (var authorizationUser in data)
					{
						OriginalAuthorizationUsers.Add(authorizationUser);
					}
				});
		}

		private void HandleDeleteRoleFromUser(object parameter)
		{
			var role = parameter as string;
			if (role == null)
				return;
			SelectedUser.Roles.Remove(role);
			AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
			OnPropertyChanged(() => AuthorizationUsers);
		}

		private void HandleDeletePermissionFromUser(object parameter)
		{
			var permission = parameter as OperationPermission;
			if (permission == null)
				return;

			SelectedUser.Permissions.Remove(permission);
			AuthorizationUsers = new ObservableCollection<AuthorizationUser>(AuthorizationUsers);
			OnPropertyChanged(() => AuthorizationUsers);
		}

		private void HandleDeletePermissionFromRole(object parameter)
		{
			var permission = parameter as OperationPermission;
			if (permission == null)
				return;

			SelectedRole.Permissions.Remove(permission);
			AuthorizationRoles = new ObservableCollection<AuthorizationRole>(AuthorizationRoles);
			OnPropertyChanged(() => AuthorizationRoles);
		}

		private void HandleDeleteAuthorizationRole(object parameter)
		{
			var role = parameter as AuthorizationRole;
			if (role == null)
				return;
			AuthorizationRoles.Remove(role);
			SelectedRole = null;
		}

		private void HandleDeleteAuthorizationUser(object parameter)
		{
			var user = parameter as AuthorizationUser;
			if (user == null)
				return;
			AuthorizationUsers.Remove(user);
			SelectedUser = null;
		}

		public override void LoadFor(DatabaseDocument _)
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name);
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

					AuthorizationUsers.CollectionChanged += (sender, args) => HasUnsavedChanges = true;
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

					AuthorizationRoles.CollectionChanged += (sender, args) => HasUnsavedChanges = true;
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