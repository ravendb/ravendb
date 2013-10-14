using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Extensions;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
	public class ScriptedIndexSettingsSectionModel : SettingsSectionModel
	{
		private static readonly ISyntaxLanguage IndexLanguage;
		private static readonly ISyntaxLanguage DeleteLanguage;

		public BindableCollection<string> AvailableIndexes { get; private set; }
		public Dictionary<string, ScriptedIndexResults> ScriptedIndexes { get; private set; }
		public Dictionary<string, ScriptedIndexResults> OriginalScriptedIndexes { get; private set; }
		
		public ScriptedIndexResults SelectedScript
		{
			get { return selectedScript; }
			set
			{
				selectedScript = value;
				SelectedScriptChanged();
			}
		}
		public List<string> IndexItem { get; set; }

		public EditorDocument IndexScript { get; private set; }
		public EditorDocument DeleteScript { get; private set; }

		static ScriptedIndexSettingsSectionModel()
		{
			IndexLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
			DeleteLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
		}

		public Observable<RavenJObject> DocumentToSample { get; set; } 

		public ScriptedIndexSettingsSectionModel()
		{
			DocumentToSample = new Observable<RavenJObject>();
			AvailableIndexes = new BindableCollection<string>(x => x);
			SectionName = "Scripted Index";			
			ScriptedIndexes = new Dictionary<string, ScriptedIndexResults>();
			OriginalScriptedIndexes = new Dictionary<string, ScriptedIndexResults>();
			IndexItem = new List<string>();
			IndexScript = new EditorDocument { Language = IndexLanguage };
			DeleteScript = new EditorDocument { Language = DeleteLanguage };
			UpdateAvailableIndexes();
			LoadScriptForIndex();
			IndexScript.Language.RegisterService(new ScriptIndexIntelliPromptProvider(DocumentToSample));
			DeleteScript.Language.RegisterService(new ScriptIndexIntelliPromptProvider(DocumentToSample, false));
		}

		public override void CheckForChanges()
		{
			if(HasUnsavedChanges)
				return;

			if (ScriptedIndexes.Count != OriginalScriptedIndexes.Count)
			{
				HasUnsavedChanges = true;
				return;
			}

			foreach (var scriptedIndexResultse in ScriptedIndexes)
			{
				if (scriptedIndexResultse.Value == null)
				{
					if (OriginalScriptedIndexes[scriptedIndexResultse.Key] != null)
					{
						HasUnsavedChanges = true;
						return;
					}
				}
				else if (scriptedIndexResultse.Value.Equals(OriginalScriptedIndexes[scriptedIndexResultse.Key]) == false)
				{
					HasUnsavedChanges = true;
					return;
				}
			}
		}

		public override void MarkAsSaved()
		{
			HasUnsavedChanges = false;

			OriginalScriptedIndexes = ScriptedIndexes;
		}

		private void LoadScriptForIndex()
		{
			if (IndexName == null)
				return;
			
			if (ScriptedIndexes.ContainsKey(indexName))
			{
				SelectedScript = ScriptedIndexes[indexName];
				return;
			}

			var id = ScriptedIndexResults.IdPrefix + indexName;
			ApplicationModel.DatabaseCommands.GetAsync(id)
			                .ContinueOnSuccessInTheUIThread(doc =>
			                {
				                if (doc == null)
				                {
					                ScriptedIndexes[indexName] = null;
					                OriginalScriptedIndexes[indexName] = null;
				                }
				                else
				                {
					                ScriptedIndexes[indexName] =
						                ApplicationModel.CreateSerializer()
						                                .Deserialize<ScriptedIndexResults>(new RavenJTokenReader(doc.DataAsJson));
									OriginalScriptedIndexes[indexName] = ApplicationModel.CreateSerializer()
														.Deserialize<ScriptedIndexResults>(new RavenJTokenReader(doc.DataAsJson));
				                }

				                SelectedScript = ScriptedIndexes[indexName];
			                });
		}

		private void SelectedScriptChanged()
		{
			OnPropertyChanged(() => SelectedScript);

			if (SelectedScript != null)
			{
				IndexScript.SetText(SelectedScript.IndexScript);
				DeleteScript.SetText(SelectedScript.DeleteScript);
				OnPropertyChanged(() => IndexScript);
				OnPropertyChanged(() => DeleteScript);
			}
		}

		private string indexName;
		private ScriptedIndexResults selectedScript;
		public string IndexName
		{
			get
			{
				return indexName;
			}
			set
			{
				if (string.IsNullOrWhiteSpace(value))
					return;

				if (indexName != null)
					StoreChanges();

				indexName = value;
				LoadScriptForIndex();
				UpdateIntelli();
				OnPropertyChanged(() => IndexName);
			}
		}
		public ICommand CreateScript
		{
			get
			{
				return new ActionCommand(() =>
				{
					var id = ScriptedIndexResults.IdPrefix + indexName;
					ScriptedIndexes[IndexName] = new ScriptedIndexResults {Id = id};
					SelectedScript = ScriptedIndexes[IndexName];
					if (DeletedIndexes.Contains(IndexName))
						DeletedIndexes.Remove(IndexName);
				});
			}
		}

		public List<string> DeletedIndexes = new List<string>(); 

		public ICommand RemoveScript
		{
			get { return new ActionCommand(() =>
			{
				SelectedScript = null;
				ScriptedIndexes.Remove(indexName);
				DeletedIndexes.Add(indexName);
			});}
		}

		private void UpdateIntelli()
		{
			DatabaseCommands.GetIndexAsync(IndexName)
				.ContinueOnUIThread(task =>
				{
					if (task.IsFaulted || task.Result == null)
					{
						return;
					}

					IndexItem = task.Result.Fields.ToList();
					UpdateDocumentToSample();
				}).Catch();
		}

		private void UpdateDocumentToSample()
		{
			DocumentToSample.Value = new RavenJObject();
			foreach (var item in IndexItem)
			{
				DocumentToSample.Value.Add(item, new RavenJObject());
			}
		}

		public void StoreChanges()
		{
			if (SelectedScript == null)
				return;
			SelectedScript.IndexScript = IndexScript.Text;
			SelectedScript.DeleteScript = DeleteScript.Text;
		}

		protected override void OnViewLoaded()
		{
			base.OnViewLoaded();

			Database.ObservePropertyChanged()
						 .TakeUntil(Unloaded)
						 .Subscribe(_ =>
						 {
							 UpdateAvailableIndexes();

							 Database.Value.Statistics.ObservePropertyChanged()
									 .TakeUntil(
										 Database.ObservePropertyChanged().Select(__ => Unit.Default).Amb(Unloaded))
									 .Subscribe(__ => UpdateAvailableIndexes());
						 });

			UpdateAvailableIndexes();
		}

		private void UpdateAvailableIndexes()
		{
			if (Database.Value == null || Database.Value.Statistics.Value == null)
			{
				return;
			}

			AvailableIndexes.Match(Database.Value.Statistics.Value.Indexes.Select(i => i.Id.ToString()).ToArray());
			if (IndexName == null)
				IndexName = AvailableIndexes.FirstOrDefault();
		}
	}
}
