using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection.Async;
using Raven.Database.Bundles.SqlReplication;
using Raven.Json.Linq;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Settings;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class SqlReplicationSettingsSectionModel : SettingsSectionModel
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
			SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfig>();
			SelectedReplication = new Observable<SqlReplicationConfig>();
			FirstItemOfCollection = new Observable<RavenJObject>();
			script = new EditorDocument { Language = JScriptLanguage };
			Script.Language.RegisterService(new SqlReplicationScriptIntelliPromptProvider(FirstItemOfCollection));

			script.TextChanged += (sender, args) => UpdateScript();
			SelectedReplication.PropertyChanged += (sender, args) => UpdateParameters();
			SectionName = "Sql Replication";
		}

		private void UpdateAvailableFactoryNames()
		{
			AvilableFactoryNames = new ObservableCollection<string>
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

		private void UpdateScript()
		{
			if (SelectedReplication.Value == null)
				return;

			SelectedReplication.Value.Script = ScriptData;
		}

		public ObservableCollection<string> AvilableFactoryNames { get; set; }
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

		private void UpdateParameters()
		{
			OnPropertyChanged(() => SqlReplicationConfigs);
			if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionString) == false)
				SelectedConnectionStringIndex = 0;
			else if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionStringName) == false)
				SelectedConnectionStringIndex = 1;
			else if (string.IsNullOrWhiteSpace(SelectedReplication.Value.ConnectionStringName) == false)
				SelectedConnectionStringIndex = 2;
			else
				SelectedConnectionStringIndex = 0;

			OnPropertyChanged(() => SelectedConnectionStringIndex);

			ScriptData = SelectedReplication.Value.Script;
			OnPropertyChanged(() => Script);

			if (!string.IsNullOrWhiteSpace(SelectedReplication.Value.RavenEntityName))
			{
				SelectedCollectionIndex = AvailableObjects.IndexOf(SelectedReplication.Value.RavenEntityName);
			}
			else
				SelectedCollectionIndex = -1;

			OnPropertyChanged(() => SelectedCollectionIndex);
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
						new ActionCommand(() => SqlReplicationConfigs.Add(new SqlReplicationConfig { Name = "Temp_Name" })));
			}
		}

		public Observable<SqlReplicationConfig> SelectedReplication { get; set; }
		IEditorDocument script;
		private int selectedCollectionIndex;
		public IEditorDocument Script
		{
			get
			{
				return script;
			}
		}

		public int SelectedConnectionStringIndex { get; set; }
		public int SelectedCollectionIndex
		{
			get { return selectedCollectionIndex; }
			set
			{
				selectedCollectionIndex = value;
				if (value >= 0)
				{
					SelectedReplication.Value.RavenEntityName = AvailableObjects[selectedCollectionIndex];
					ApplicationModel.DatabaseCommands.QueryAsync(CollectionsIndex, new IndexQuery
					                                                               {
						                                                               Query =
							                                                               "Tag:" +
							                                                               AvailableObjects[selectedCollectionIndex],
						                                                               PageSize = 1
					                                                               }, null).ContinueOnSuccessInTheUIThread(result =>
					                                                               {
						                                                               FirstItemOfCollection.Value = result.Results.FirstOrDefault();
					                                                               });
				}
			}
		}
		protected Observable<RavenJObject> FirstItemOfCollection { get; set; }

		protected string ScriptData
		{
			get { return Script.CurrentSnapshot.Text; }
			set { Script.SetText(value); }
		}

		public ObservableCollection<SqlReplicationConfig> SqlReplicationConfigs { get; set; }

		private void HandleDeleteReplication(object parameter)
		{
			if (SelectedReplication.Value == null)
				return;

			SqlReplicationConfigs.Remove(SelectedReplication.Value);
			SelectedReplication.Value = null;
			OnPropertyChanged(() => SelectedReplication);
		}

		public override void LoadFor(DatabaseDocument database)
		{
			ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(database.Id)
				.Advanced.LoadStartingWithAsync<SqlReplicationConfig>("Raven/SqlReplication/Configuration/")
				.ContinueOnSuccessInTheUIThread(documents =>
				{
					if (documents == null)
						return;

					SqlReplicationConfigs = new ObservableCollection<SqlReplicationConfig>(documents);
				});
		}

		public void UpdateIds()
		{
			foreach (var sqlReplicationConfig in SqlReplicationConfigs)
			{
				sqlReplicationConfig.Id = "Raven/SqlReplication/Configuration/" + sqlReplicationConfig.Name;
			}
		}
	}
}
