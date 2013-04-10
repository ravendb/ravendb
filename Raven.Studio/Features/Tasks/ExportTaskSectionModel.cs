using System.Collections.ObjectModel;
using System.Windows.Controls;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Smuggler;
using Raven.Studio.Commands;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Tasks
{
	public class ExportTaskSectionModel : TaskSectionModel
	{
		protected static ISyntaxLanguage JScriptLanguage { get; set; }

		static ExportTaskSectionModel()
		{
			JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
		}

		public ExportTaskSectionModel()
		{
			Name = "Export Database";
		    IconResource = "Image_Export_Tiny";
			Description = "Export your database to a dump file. Both indexes and documents are exported.";
			Options = new Observable<SmugglerOptions>{Value = new SmugglerOptions()};
			Filters = new ObservableCollection<FilterSetting>();
			IncludeDocuments = new Observable<bool>{Value = true};
			IncludeIndexes = new Observable<bool>{Value = true};
			IncludeAttachments = new Observable<bool>();
			IncludeTransforms = new Observable<bool>{Value = true};
			script = new EditorDocument { Language = JScriptLanguage };
			script.TextChanged += (sender, args) => UpdateScript();
		}

		public Observable<SmugglerOptions> Options { get; set; }
		public ObservableCollection<FilterSetting> Filters { get; set; } 
		public Observable<bool> IncludeDocuments { get; set; }
		public Observable<bool> IncludeIndexes { get; set; }
		public Observable<bool> IncludeAttachments { get; set; }
		public Observable<bool> IncludeTransforms { get; set; }

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

		public override ICommand Action
		{
			get { return new ExportDatabaseCommand(this, line => Execute.OnTheUI(() => Output.Add(line))); }
		}

		IEditorDocument script;
		public IEditorDocument Script
		{
			get
			{
				return script;
			}
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
}