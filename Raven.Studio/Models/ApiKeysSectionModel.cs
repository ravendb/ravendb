using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Studio.Behaviors;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class ApiKeysSectionModel : SettingsSectionModel, IAutoCompleteSuggestionProvider
	{
		public ApiKeysSectionModel()
		{
			SectionName = "Api Keys";

			OriginalApiKeys = new ObservableCollection<ApiKeyDefinition>();
			ApiKeys = new ObservableCollection<ApiKeyDefinition>();

			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();
			session.Advanced.LoadStartingWithAsync<ApiKeyDefinition>("Raven/ApiKeys/").ContinueOnSuccessInTheUIThread(
				apiKeys =>
				{
					OriginalApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys);
					ApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys);
					OnPropertyChanged(() => ApiKeys);
				});
		}

		public ObservableCollection<ApiKeyDefinition> ApiKeys { get; set; }
		public ObservableCollection<ApiKeyDefinition> OriginalApiKeys { get; set; }
		public string SearchApiKeys { get; set; }

		public ApiKeyDefinition SelectedApiKey { get; set; }

		public ICommand AddApiKeyCommand
		{
			get { return new ActionCommand(() => ApiKeys.Add(new ApiKeyDefinition())); }
		}

		public ICommand DeleteApiKey
		{
			get { return new ActionCommand(DeleteApi); }
		}

		public ICommand GenerateSecretCommand {get{return new ActionCommand(GenerateSecret);}}

		public ICommand AddDatabaseAccess
		{
			get
			{
				return new ActionCommand(() =>
				{
					SelectedApiKey.Databases.Add(new DatabaseAccess());
					Update();
				});
			}
		}

		public ICommand DeleteDatabaseAccess { get { return new ActionCommand(DeleteDatabaseAccessCommand); } }

		public ICommand Search { get { return new ActionCommand(SearchApiKeysCommand); } }

		private void SearchApiKeysCommand()
		{
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();
			session.Advanced.LoadStartingWithAsync<ApiKeyDefinition>("Raven/ApiKeys/" + SearchApiKeys).ContinueOnSuccessInTheUIThread(
				apiKeys =>
				{
					OriginalApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys);
					ApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys);
					OnPropertyChanged(() => ApiKeys);
				});
		}

		private void DeleteDatabaseAccessCommand(object parameter)
		{
			var access = parameter as DatabaseAccess;
			if (access == null)
				return;

			SelectedApiKey.Databases.Remove(access);

			Update();
		}

		private void DeleteApi(object parameter)
		{
			var key = parameter as ApiKeyDefinition;
			ApiKeys.Remove(key ?? SelectedApiKey);

			Update();
		}

		private void GenerateSecret(object parameter)
		{
			var key = parameter as ApiKeyDefinition;
			if(key == null)
				return;
			key.Secret = Base62Util.ToBase62(Guid.NewGuid()).Replace("-", "");
			Update();
		}

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			var list = ApplicationModel.Current.Server.Value.Databases.Cast<object>().ToList();
			list.Add("*");
			return TaskEx.FromResult<IList<object>>(list);
		}

		public void Update()
		{
			ApiKeys = new ObservableCollection<ApiKeyDefinition>(ApiKeys);
			OnPropertyChanged(() => ApiKeys);
		}
	}
}