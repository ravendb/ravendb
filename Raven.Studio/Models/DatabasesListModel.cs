using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Studio.Behaviors;
using Raven.Studio.Commands;
using Raven.Studio.Extensions;
using Raven.Studio.Infrastructure;
using System.Linq;

namespace Raven.Studio.Models
{
    public class DatabasesListModel : PageViewModel, IAutoCompleteSuggestionProvider
    {
        private readonly ChangeDatabaseCommand changeDatabase;

        public DatabasesListModel()
        {
            ModelUrl = "/databases";
            changeDatabase = new ChangeDatabaseCommand();
            DatabasesWithEditableBundles = new List<string>();
            Databases = new BindableCollection<DatabaseListItemModel>(m => m.Name);
            ApplicationModel.Current.Server.Value.SelectedDatabase.PropertyChanged += (sender, args) =>
            {
                OnPropertyChanged(() => SelectedDatabase);
                OnPropertyChanged(() => ShowEditBundles);
            };

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

        public BindableCollection<DatabaseListItemModel> Databases { get; private set; }
        public List<string> DatabasesWithEditableBundles { get; set; }

        public DatabaseListItemModel SelectedDatabase
        {
            get
            {
                return ApplicationModel.Database.Value != null
                           ? Databases.FirstOrDefault(
                               m =>
                               m.Name.Equals(ApplicationModel.Database.Value.Name, StringComparison.InvariantCulture))
                           : null;
            }
            set
            {
                if (value == null)
                    return;

                if (changeDatabase.CanExecute(value.Name))
                    changeDatabase.Execute(value.Name);
            }
        }

        public bool ShowEditBundles
        {
            get { return SelectedDatabase != null && DatabasesWithEditableBundles.Contains(SelectedDatabase.Name); }
        }

        public ICommand DeleteSelectedDatabase
        {
            get { return new DeleteDatabaseCommand(this); }
        }

        public override Task TimerTickedAsync()
        {
            return TaskEx.WhenAll(
                new[] {ApplicationModel.Current.Server.Value.TimerTickedAsync()}
                    // Fetch databases names from the server
                    .Concat(Databases.Select(m => m.TimerTickedAsync()))); // Refresh statistics
        }

        protected override void OnViewLoaded()
        {
            ApplicationModel.Current.Server.Value.SelectedDatabase
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ => OnPropertyChanged(() => SelectedDatabase));

            ApplicationModel.Current.Server.Value.Databases.ObserveCollectionChanged()
                .Throttle(TimeSpan.FromMilliseconds(10))
                .ObserveOnDispatcher()
                .TakeUntil(Unloaded)
                .Subscribe(_ => RefreshDatabaseList());

            RefreshDatabaseList();
        }

        private void RefreshDatabaseList()
        {
            Databases.Match(
                ApplicationModel.Current.Server.Value.Databases.Where(s => s != Constants.SystemDatabase).Select(
                    name => new DatabaseListItemModel(name)).ToArray());
            OnPropertyChanged(() => SelectedDatabase);
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

        public ICommand SaveChanges{get{return new ActionCommand(SaveApiKeys);}}

        public ICommand AddDatabaseAccess
        {
            get
            {
                return new ActionCommand(() =>
                {
                    SelectedApiKey.Databases.Add(new DatabaseAccess());
                    ApiKeys = new ObservableCollection<ApiKeyDefinition>(ApiKeys);
                    OnPropertyChanged(() => ApiKeys);
                });
            }
        }

        public ICommand DeleteDatabaseAccess { get { return new ActionCommand(DeleteDatabaseAccessCommand); } }

        public ICommand Search{get{return new ActionCommand(SearchApiKeysCommand);}}

        private void SearchApiKeysCommand()
        {
            var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();
            session.Advanced.LoadStartingWithAsync<ApiKeyDefinition>("Raven/ApiKeys/").ContinueOnSuccessInTheUIThread(
                apiKeys =>
                {
                    OriginalApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys.Where(definition => definition.Name.IndexOf(SearchApiKeys, StringComparison.InvariantCultureIgnoreCase) >=0));
                    ApiKeys = new ObservableCollection<ApiKeyDefinition>(apiKeys.Where(definition => definition.Name.IndexOf(SearchApiKeys, StringComparison.InvariantCultureIgnoreCase) >=0));
                    OnPropertyChanged(() => ApiKeys);
                });
        }

        private void DeleteDatabaseAccessCommand(object parameter)
        {
            var access = parameter as DatabaseAccess;
            if(access == null)
                return;

            SelectedApiKey.Databases.Remove(access);

            ApiKeys = new ObservableCollection<ApiKeyDefinition>(ApiKeys);
            OnPropertyChanged(() => ApiKeys);
        }

        private void DeleteApi(object parameter)
        {
            var key = parameter as ApiKeyDefinition;
            ApiKeys.Remove(key ?? SelectedApiKey);

            ApiKeys = new ObservableCollection<ApiKeyDefinition>(ApiKeys);
            OnPropertyChanged(() => ApiKeys);
        }

        private void SaveApiKeys()
        {
			var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();

            var apiKeysToDelete = OriginalApiKeys
                  .Where(apiKeyDefinition => ApiKeys.Contains(apiKeyDefinition) == false)
                  .ToList();

            foreach (var apiKeyDefinition in apiKeysToDelete)
            {
                DatabaseCommands.ForDefaultDatabase().DeleteDocumentAsync(apiKeyDefinition.Id);
            }

            foreach (var apiKeyDefinition in ApiKeys)
            {
                apiKeyDefinition.Id = "Raven/ApiKeys/" + apiKeyDefinition.Name;
                //apiKeyDefinition.Secret = Convert.ToBase64String(Encoding.UTF8.GetBytes(apiKeyDefinition.Secret));
                session.Store(apiKeyDefinition);
            }

            session.SaveChangesAsync();
            ApiKeys = new ObservableCollection<ApiKeyDefinition>(ApiKeys);
            OnPropertyChanged(() => ApiKeys);
            ApplicationModel.Current.AddInfoNotification("Api Keys Saved");
        }

        public Task<IList<object>> ProvideSuggestions(string enteredText)
        {
            return new Task<IList<object>>(() => Databases.Select(model => model.Name).Cast<object>().ToList());
        }
    }
}