using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Server.Security.Windows;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class WindowsAuthSettingsSectionModel : SettingsSectionModel
	{
		private int selectedTab;

		public WindowsAuthSettingsSectionModel()
		{
			SectionName = "Windows Authentication";
			Document = new Observable<WindowsAuthDocument>();
			RequiredUsers = new ObservableCollection<WindowsAuthData>();
			RequiredGroups = new ObservableCollection<WindowsAuthData>();
			OriginalRequiredUsers = new ObservableCollection<WindowsAuthData>();
			OriginalRequiredGroups = new ObservableCollection<WindowsAuthData>();
			SelectedList = new ObservableCollection<WindowsAuthData>();
			DatabaseSuggestionProvider = new DatabaseSuggestionProvider();
			WindowsAuthName = new WindowsAuthName();

			ApplicationModel.DatabaseCommands.ForSystemDatabase()
				.GetAsync("Raven/Authorization/WindowsSettings")
				.ContinueOnSuccessInTheUIThread(doc =>
				{
					if (doc == null)
					{
						Document.Value = new WindowsAuthDocument();
						return;
					}
					Document.Value = doc.DataAsJson.JsonDeserialization<WindowsAuthDocument>();
					RequiredUsers = new ObservableCollection<WindowsAuthData>(Document.Value.RequiredUsers);
					RequiredGroups = new ObservableCollection<WindowsAuthData>(Document.Value.RequiredGroups);
					OriginalRequiredUsers = new ObservableCollection<WindowsAuthData>(Document.Value.RequiredUsers);
					OriginalRequiredGroups = new ObservableCollection<WindowsAuthData>(Document.Value.RequiredGroups);
					SelectedList = RequiredUsers;
					
					OnPropertyChanged(() => Document);
					OnPropertyChanged(() => RequiredUsers);
					OnPropertyChanged(() => RequiredGroups);
				});
		}

		public override void CheckForChanges()
		{
			if(HasUnsavedChanges)
				return;

			if (OriginalRequiredGroups.Count != RequiredGroups.Count || OriginalRequiredUsers.Count != RequiredUsers.Count)
			{
				HasUnsavedChanges = true;
				return;
			}

			foreach (var windowsAuthData in RequiredUsers)
			{
				if (windowsAuthData.Equals(OriginalRequiredUsers.FirstOrDefault(data => data.Name == windowsAuthData.Name)) == false)
				{
					HasUnsavedChanges = true;
					return;
				}
			}

			foreach (var windowsAuthData in RequiredGroups)
			{
				if (windowsAuthData.Equals(OriginalRequiredGroups.FirstOrDefault(data => data.Name == windowsAuthData.Name)) == false)
				{
					HasUnsavedChanges = true;
					return;
				}
			}
		}

		public WindowsAuthName WindowsAuthName { get; set; }
		public DatabaseSuggestionProvider DatabaseSuggestionProvider { get; set; }
		public Observable<WindowsAuthDocument> Document { get; set; }
		public ObservableCollection<WindowsAuthData> RequiredUsers { get; set; }
		public ObservableCollection<WindowsAuthData> RequiredGroups { get; set; }
		public ObservableCollection<WindowsAuthData> OriginalRequiredUsers { get; set; }
		public ObservableCollection<WindowsAuthData> OriginalRequiredGroups { get; set; }
		public WindowsAuthData SelectedItem { get; set; }
		public int SelectedTab
		{
			get { return selectedTab; }
			set
			{
				selectedTab = value;
				SelectedList = selectedTab == 0 ? RequiredUsers : RequiredGroups;
			}
		}

		private ObservableCollection<WindowsAuthData> SelectedList { get; set; } 

		public ICommand AddUser { get { return new ActionCommand(() => RequiredUsers.Add(new WindowsAuthData())); } }
		public ICommand AddGroup { get { return new ActionCommand(() => RequiredGroups.Add(new WindowsAuthData())); } }
		public ICommand DeleteEntry{get{return new ActionCommand(() =>
		{
			SelectedList.Remove(SelectedItem);

			SelectedItem = null;
		});}}

		public ICommand AddDatabaseAccess
		{
			get
			{
				return new ActionCommand(() =>
				{
					SelectedItem.Databases.Add(new DatabaseAccess());
					SelectedList = new ObservableCollection<WindowsAuthData>(SelectedList);
					RequiredUsers = new ObservableCollection<WindowsAuthData>(RequiredUsers);
					RequiredGroups = new ObservableCollection<WindowsAuthData>(RequiredGroups);
					OnPropertyChanged(() => RequiredUsers);
					OnPropertyChanged(() => RequiredGroups);
				});
			}
		}

		public ICommand DeleteDatabaseAccess { get { return new ActionCommand(DeleteDatabaseAccessCommand); } }

		private void DeleteDatabaseAccessCommand(object parameter)
		{
			var access = parameter as DatabaseAccess;
			if (access == null)
				return;

			SelectedItem.Databases.Remove(access);

			SelectedList = new ObservableCollection<WindowsAuthData>(SelectedList);
			RequiredUsers = new ObservableCollection<WindowsAuthData>(RequiredUsers);
			RequiredGroups = new ObservableCollection<WindowsAuthData>(RequiredGroups);
			OnPropertyChanged(() => RequiredUsers);
			OnPropertyChanged(() => RequiredGroups);
		}

		public override void MarkAsSaved()
		{
			throw new System.NotImplementedException();
		}
	}

	public class DatabaseSuggestionProvider : IAutoCompleteSuggestionProvider
	{
		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			var list = ApplicationModel.Current.Server.Value.Databases.Cast<object>().ToList();
			list.Add("*");
			return TaskEx.FromResult<IList<object>>(list);
		}
	}

	public class WindowsAuthName : IAutoCompleteSuggestionProvider
	{
		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return TaskEx.FromResult<IList<object>>(new List<object>
			{
				@"NT AUTHORITY\Network Service", 
				@"IIS AppPool\DefaultAppPool",
				@"NT AUTHORITY\Local Service", 
				@"NT AUTHORITY\System", 
			});
		}
	}
}
