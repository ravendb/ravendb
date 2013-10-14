using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Database.Bundles.SqlReplication;
using Raven.Json.Linq;
using Raven.Studio.Behaviors;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Bundles;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class SqlReplicationSettingsSectionModel : SettingsSectionModel, IAutoCompleteSuggestionProvider
	{
		private ICommand addReplicationCommand;
		private ICommand deleteReplicationCommand;
		private const string CollectionsIndex = "Raven/DocumentsByEntityName";

		static SqlReplicationSettingsSectionModel()
		{
			JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
		}

		protected static ISyntaxLanguage JScriptLanguage { get; set; }

		public SqlReplicationSettingsSectionModel()
		{
			UpdateAvailableFactoryNames();
			AvailableObjects = new ObservableCollection<string>();
			UpdateAvailableCollections();
			SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfigModel>();
			OriginalSqlReplicationConfigs = new ObservableCollection<SqlReplicationConfigModel>();
			SelectedReplication = new Observable<SqlReplicationConfigModel>();
			SelectedTable = new Observable<SqlReplicationTable>();
			FirstItemOfCollection = new Observable<RavenJObject>();
			script = new EditorDocument { Language = JScriptLanguage };
			Script.Language.RegisterService(new SqlReplicationScriptIntelliPromptProvider(FirstItemOfCollection, this));

			script.TextChanged += (sender, args) => UpdateScript();
			SelectedReplication.PropertyChanged += (sender, args) => UpdateParameters();
			SectionName = "Sql Replication";
		}

		private void UpdateAvailableFactoryNames()
		{
			AvailableFactoryNames = new ObservableCollection<string>
			{
				"System.Data.SqlClient",
				"System.Data.SqlServerCe.4.0",
				"System.Data.OleDb",
				"System.Data.OracleClient",
				"MySql.Data.MySqlClient",
				"System.Data.SqlServerCe.3.5",
				"Npgsql"
			};
		}

		public override void CheckForChanges()
		{
			if(HasUnsavedChanges)
				return;

			if (OriginalSqlReplicationConfigs.Count != SqlReplicationConfigs.Count)
			{
				HasUnsavedChanges = true;
				return;
			}

			foreach (var sqlReplicationConfigModel in SqlReplicationConfigs)
			{
				if (
					sqlReplicationConfigModel.Equals(
						OriginalSqlReplicationConfigs.FirstOrDefault(model => model.Name == sqlReplicationConfigModel.Name)) == false)
				{
					HasUnsavedChanges = true;
					return;
				}
			}
		}

		public override void MarkAsSaved()
		{
			HasUnsavedChanges = false;

			OriginalSqlReplicationConfigs = SqlReplicationConfigs;
		}

		private void UpdateScript()
		{
			if (SelectedReplication.Value == null)
				return;

			SelectedReplication.Value.Script = ScriptData;
		}

		public ObservableCollection<string> AvailableFactoryNames { get; set; }
		public ObservableCollection<string> AvailableObjects { get; private set; }
		private void UpdateAvailableCollections()
		{
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
				CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccessInTheUIThread(collections =>
				{
					AvailableObjects.Clear();
					AvailableObjects.AddRange(collections.OrderByDescending(x => x.Count)
												  .Where(x => x.Count > 0)
												  .Select(col => col.Name).ToList());

					OnPropertyChanged(() => AvailableObjects);
				});
		}

		public ICommand DeleteTable
		{
			get
			{
				return new ActionCommand(() =>
				{
					var sqlReplicationConfigModel = SelectedReplication.Value;
					if (sqlReplicationConfigModel == null)
						return;
					sqlReplicationConfigModel.SqlReplicationTables.Remove(SelectedTable.Value);
					SelectedTable.Value = null;
				});
			}
		}

		public ICommand AddTable
		{
			get
			{
				return new ActionCommand(() =>
				{
					if (SelectedReplication.Value == null)
						return;
					if (SelectedReplication.Value.SqlReplicationTables == null)
						SelectedReplication.Value.SqlReplicationTables = new ObservableCollection<SqlReplicationTable>();

					SelectedReplication.Value.SqlReplicationTables.Add(new SqlReplicationTable());
				});
			}
		}

		private void UpdateParameters()
		{
			if (SelectedReplication.Value == null)
			{
				return;
			}

			if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionString) == false)
				SelectedConnectionStringIndex = 0;
			else if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionStringName) == false)
				SelectedConnectionStringIndex = 1;
			else if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionStringName) == false)
				SelectedConnectionStringIndex = 2;
			else
				SelectedConnectionStringIndex = 0;

			ScriptData = SelectedReplication.Value.Script;
		}

		public ICommand DeleteReplication
		{
			get { return deleteReplicationCommand ?? (deleteReplicationCommand = new ActionCommand(HandleDeleteReplication)); }
		}

		public ICommand AddReplication
		{
			get
			{
				return addReplicationCommand ??
					   (addReplicationCommand =
						new ActionCommand(() =>
						{
							var model = new SqlReplicationConfigModel { Name = "" };
							SqlReplicationConfigs.Add(model);
							SelectedReplication.Value = model;
						}));
			}
		}

		public Observable<SqlReplicationConfigModel> SelectedReplication { get; set; }
		public Observable<SqlReplicationTable> SelectedTable { get; set; }
		IEditorDocument script;
		private int selectedConnectionStringIndex;
		public IEditorDocument Script
		{
			get
			{
				return script;
			}
		}

		public int SelectedConnectionStringIndex
		{
			get { return selectedConnectionStringIndex; }
			set
			{
				selectedConnectionStringIndex = value;
				OnPropertyChanged(() => SelectedConnectionStringIndex);
			}
		}

		protected Observable<RavenJObject> FirstItemOfCollection { get; set; }

		protected string ScriptData
		{
			get { return Script.CurrentSnapshot.Text; }
			set { Script.SetText(value); }
		}

		public ObservableCollection<SqlReplicationConfigModel> SqlReplicationConfigs { get; set; }
		public ObservableCollection<SqlReplicationConfigModel> OriginalSqlReplicationConfigs { get; set; }

		private void HandleDeleteReplication(object parameter)
		{
			var replication = parameter as SqlReplicationConfigModel ?? SelectedReplication.Value;

			if (replication == null)
				return;

			if (replication == SelectedReplication.Value)
			{
				SelectedReplication.Value = null;
			}

			SqlReplicationConfigs.Remove(replication);
		}

		public override void LoadFor(DatabaseDocument _)
		{
			ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name)
				.Advanced.LoadStartingWithAsync<SqlReplicationConfig>("Raven/SqlReplication/Configuration/")
				.ContinueOnSuccessInTheUIThread(documents =>
				{
					if (documents == null)
						return;

					SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfigModel>();
					foreach (var doc in documents)
					{
						SqlReplicationConfigs.Add(SqlReplicationConfigModel.FromSqlReplicationConfig(doc));
						OriginalSqlReplicationConfigs.Add(SqlReplicationConfigModel.FromSqlReplicationConfig(doc));
					}
					if (SqlReplicationConfigs.Any())
					{
						SelectedReplication.Value = SqlReplicationConfigs.FirstOrDefault();
					}
				});
		}

		public void UpdateIds()
		{
			foreach (var sqlReplicationConfig in SqlReplicationConfigs)
			{
				sqlReplicationConfig.Id = "Raven/SqlReplication/Configuration/" + sqlReplicationConfig.Name;
			}
		}

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			return TaskEx.FromResult<IList<object>>(AvailableObjects.Cast<object>().ToList());
		}
	}
}
