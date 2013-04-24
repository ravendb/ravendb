using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Microsoft.Expression.Interactivity.Core;
using Raven.Client.Connection.Async;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Abstractions.Smuggler;

namespace Raven.Studio.Features.Tasks
{
	public abstract class SmugglerTaskSectionModel : TaskSectionModel
	{
		protected static ISyntaxLanguage JScriptLanguage { get; set; }

		static SmugglerTaskSectionModel()
		{
			JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
		}

		public SmugglerTaskSectionModel()
		{
			Options = new Observable<SmugglerOptions> {Value = new SmugglerOptions()};
			Filters = new ObservableCollection<FilterSetting>();
			IncludeDocuments = new Observable<bool> {Value = true};
			IncludeIndexes = new Observable<bool> {Value = true};
			IncludeAttachments = new Observable<bool>();
			IncludeTransforms = new Observable<bool> {Value = true};
			UseCollections = new Observable<bool>();
			script = new EditorDocument {Language = JScriptLanguage};
			script.TextChanged += (sender, args) => UpdateScript();
			const string collectionsIndex = "Raven/DocumentsByEntityName";
			ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
			collectionsIndex, "Tag", "", 100)
			.ContinueOnSuccess(collections =>
			{
				Collections = new List<CollectionSelectionInfo>();
				foreach (var result in collections.Select(col => col.Name))
				{
					Collections.Add(new CollectionSelectionInfo(result));
				}
			});
		}

		public Observable<SmugglerOptions> Options { get; set; }
		public ObservableCollection<FilterSetting> Filters { get; set; }
		public Observable<bool> IncludeDocuments { get; set; }
		public Observable<bool> IncludeIndexes { get; set; }
		public Observable<bool> IncludeAttachments { get; set; }
		public Observable<bool> IncludeTransforms { get; set; }
		public Observable<bool> UseCollections { get; set; }
		public List<CollectionSelectionInfo> Collections { get; set; } 

		public ICommand AddFilter
		{
			get
			{
				return new ActionCommand(() =>
				{
					Filters.Add(new FilterSetting());
					OnPropertyChanged(() => Options);
				});
			}
		}

		public ICommand DeleteFilter
		{
			get
			{
				return new ActionCommand(param =>
				{
					var filter = param as FilterSetting;
					if (filter != null)
						Filters.Remove(filter);
				});
			}
		}

		private IEditorDocument script;
		public IEditorDocument Script
		{
			get { return script; }
		}

		private void UpdateScript()
		{
			Options.Value.TransformScript = ScriptData;
		}

		public string ScriptData
		{
			get { return Script.CurrentSnapshot.Text; }
			set { Script.SetText(value); }
		}
	}

	public class CollectionSelectionInfo
	{
		public string Name { get; set; }
		public bool Selected { get; set; }

		public CollectionSelectionInfo(string name)
		{
			Name = name;
		}
	}
}