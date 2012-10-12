using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Studio.Behaviors;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Features.Query;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Models
{
	public class PatchModel : ViewModel, IAutoCompleteSuggestionProvider
	{
		private PatchOnOptions patchOn;

        private IEditorDocument originalDoc;
		private IEditorDocument newDoc;

	    private IEditorDocument queryDoc;
	    private string selectedItem;

	    private static JsonSyntaxLanguageExtended JsonLanguage;
	    private static ISyntaxLanguage JScriptLanguage;
	    private static ISyntaxLanguage QueryLanguage;


	    static PatchModel()
        {
            JsonLanguage = new JsonSyntaxLanguageExtended();
            JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
            QueryLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
        }

        public PatchModel()
        {
            OriginalDoc = new EditorDocument()
            {
                Language = JsonLanguage,
                IsReadOnly = true,
            };

            NewDoc = new EditorDocument()
            {
                Language = JsonLanguage,
                IsReadOnly = true,
            };

            Script = new EditorDocument()
            {
                Language = JScriptLanguage
            };

            QueryDoc = new EditorDocument()
            {
                Language = QueryLanguage
            };

            ShowBeforeAndAfterPrompt = true;
            AvailableObjects = new ObservableCollection<string>();

            queryCollectionSource = new QueryDocumentsCollectionSource();
            QueryResults = new DocumentsModel(queryCollectionSource) { Header = "Matching Documents", MinimalHeader = true, HideItemContextMenu = true};
        }

        private QueryIndexAutoComplete queryIndexAutoComplete;
	    private QueryDocumentsCollectionSource queryCollectionSource;
	    private bool showBeforeAndAfterPrompt;

	    protected QueryIndexAutoComplete QueryIndexAutoComplete
        {
            get { return queryIndexAutoComplete; }
            set
            {
                queryIndexAutoComplete = value;
                QueryDoc.Language.UnregisterService<ICompletionProvider>();
                QueryDoc.Language.RegisterService<ICompletionProvider>(value.CompletionProvider);
            }
        }

        public DocumentsModel QueryResults { get; private set; }

		public PatchOnOptions PatchOn
		{
			get { return patchOn; }
			set
			{
				patchOn = value;
				OnPropertyChanged(() => PatchOn);
				OnPropertyChanged(() => IsQueryVisible);
				OnPropertyChanged(() => IsComboBoxVisible);
				OnPropertyChanged(() => BeforeAndAfterPromptText);
			    SelectedItem = "";
			    UpdateAvailableObjects();
			    ClearBeforeAndAfter();
			}
		}

	    public string BeforeAndAfterPromptText
	    {
	        get
	        {
	            switch (PatchOn)
	            {
	                case PatchOnOptions.Document:
	                    return "Press Test to try out your patch script on the selected document";
	                case PatchOnOptions.Collection:
	                case PatchOnOptions.Index:
                        return "Select a document in the list above, then press Test to try out your patch script";
	                    break;
	                default:
	                    throw new ArgumentOutOfRangeException();
	            }
	        }
	    }

	    public bool ShowBeforeAndAfterPrompt
	    {
	        get { return showBeforeAndAfterPrompt; }
            set
            {
                showBeforeAndAfterPrompt = value;
                OnPropertyChanged(() => ShowBeforeAndAfterPrompt);
            }
	    }

	    private void ClearBeforeAndAfter()
	    {
	        OriginalDoc.SetText("");
	        NewDoc.SetText("");
	        ShowBeforeAndAfterPrompt = true;
	    }

	    public ObservableCollection<string> AvailableObjects { get; private set; }
 
        public IEditorDocument QueryDoc { get; private set; }

        public IEditorDocument OriginalDoc
		{
			get { return originalDoc; }
			private set
			{
				originalDoc = value;
				OnPropertyChanged(() => OriginalDoc);
			}
		}

		public IEditorDocument NewDoc
		{
			get { return newDoc; }
			private set
			{
				newDoc = value;
				OnPropertyChanged(() => NewDoc);
			}
		}

		public string SelectedItem
		{
		    get { return selectedItem; }
            set
            {
                selectedItem = value;
                OnPropertyChanged(() => SelectedItem);
                UpdateQueryAutoComplete();
                UpdateCollectionSource();
                ClearBeforeAndAfter();
            }
		}

	    private void UpdateCollectionSource()
	    {
	        if (PatchOn == PatchOnOptions.Collection)
	        {
                queryCollectionSource.UpdateQuery(PatchModel.CollectionsIndex, new IndexQuery { Query = "Tag:" + SelectedItem });
                QueryResults.SetChangesObservable(
                        d => d.IndexChanges
                                 .Where(n => n.Name.Equals(PatchModel.CollectionsIndex, StringComparison.InvariantCulture))
                                 .Select(m => Unit.Default));
	        }
            else if (PatchOn == PatchOnOptions.Index)
            {
                queryCollectionSource.UpdateQuery(SelectedItem, new IndexQuery { Query = QueryDoc.CurrentSnapshot.Text, SkipTransformResults = true,});
                QueryResults.SetChangesObservable(
                        d => d.IndexChanges
                                 .Where(n => n.Name.Equals(SelectedItem, StringComparison.InvariantCulture))
                                 .Select(m => Unit.Default));
            }
	    }

	    private void UpdateQueryAutoComplete()
	    {
	        if (PatchOn != PatchOnOptions.Index || string.IsNullOrEmpty(SelectedItem))
	        {
	            return;
	        }

            ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexAsync(SelectedItem)
                .ContinueOnUIThread(task =>
                {
                    if (task.IsFaulted || task.Result == null)
                    {
                        return;
                    }

                    var fields = task.Result.Fields;
                    QueryIndexAutoComplete = new QueryIndexAutoComplete(fields, SelectedItem, QueryDoc);
                }).Catch();
	    }

	    public bool IsComboBoxVisible {get { return PatchOn == PatchOnOptions.Index || PatchOn == PatchOnOptions.Collection; }}

	    public bool IsQueryVisible
	    {
	        get { return PatchOn == PatchOnOptions.Index; }
	    }

	    public IEditorDocument Script { get; private set; }

		public const string CollectionsIndex = "Raven/DocumentsByEntityName";
		public ICommand Execute { get { return new ExecutePatchCommand(this); } }
		public ICommand Test { get { return new TestPatchCommand(this); } }

		public Task<IList<object>> ProvideSuggestions(string enteredText)
		{
			switch (PatchOn)
			{
				case PatchOnOptions.Document:
					return ApplicationModel.Database.Value.AsyncDatabaseCommands.StartsWithAsync(SelectedItem, 0, 25, metadataOnly: true)
						.ContinueWith(t => (IList<object>) t.Result.Select(d => d.Key).Cast<object>().ToList());

				case PatchOnOptions.Collection:
					return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
				CollectionsIndex, "Tag", "", 100)
				.ContinueOnSuccess(collections => (IList<object>)collections.OrderByDescending(x => x.Count)
											.Where(x => x.Count > 0)
											.Select(col => col.Name).Cast<object>().ToList());

				case PatchOnOptions.Index:
					return ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexNamesAsync(0, 500)
						.ContinueWith(t => (IList<object>) t.Result.Where(s => s.StartsWith(enteredText, StringComparison.InvariantCultureIgnoreCase)).Cast<object>().ToList());

				default:
					return null;
			}
		}

        private void UpdateAvailableObjects()
        {
            switch (PatchOn)
            {
                case PatchOnOptions.Collection:
                    ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetTermsCount(
                        CollectionsIndex, "Tag", "", 100)
                        .ContinueOnSuccessInTheUIThread(collections =>
                        {
                            AvailableObjects.Clear();
                            AvailableObjects.AddRange(collections.OrderByDescending(x => x.Count)
                                                          .Where(x => x.Count > 0)
                                                          .Select(col => col.Name).ToList());
                        });
                    break;
                case PatchOnOptions.Index:
                     ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexNamesAsync(0, 500)
                        .ContinueOnSuccessInTheUIThread(indexes =>
                        {
                            AvailableObjects.Clear();
                            AvailableObjects.AddRange(indexes.OrderBy(x => x));
                        });
                    break;
            }
        }

        protected override void OnViewLoaded()
        {
            base.OnViewLoaded();

            Observable.FromEventPattern<TextSnapshotChangedEventArgs>(h => QueryDoc.TextChanged += h,
                                                                         h => QueryDoc.TextChanged -= h)
                .Throttle(TimeSpan.FromSeconds(0.5))
                .TakeUntil(Unloaded)
                .ObserveOnDispatcher()
                .Subscribe(e =>
                {
                    UpdateCollectionSource();
                });
        }
	}

	public class TestPatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public TestPatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			var request = new ScriptedPatchRequest {Script = patchModel.Script.CurrentSnapshot.Text};

			switch (patchModel.PatchOn)
			{
				case PatchOnOptions.Document:
					ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(patchModel.SelectedItem).
						ContinueOnSuccessInTheUIThread(doc => patchModel.OriginalDoc.SetText(doc.ToJson().ToString()));
					//Todo: get a sample of the doc after changes (don't save the changes) and save it to NewDoc
					break;

				case PatchOnOptions.Collection:
				case PatchOnOptions.Index:
			        var selectedItem = patchModel.QueryResults.ItemSelection.GetSelectedItems().FirstOrDefault();
                    if (selectedItem == null || !selectedItem.IsRealized)
                    {
                        return;
                    }

                    patchModel.OriginalDoc.SetText(selectedItem.Item.Document.ToJson().ToString());
                    //Todo: get a sample of the doc after changes (don't save the changes) and save it to NewDoc
					break;
			}

		    patchModel.ShowBeforeAndAfterPrompt = false;
		}
	}

	public class ExecutePatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public ExecutePatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
		    AskUser.ConfirmationAsync("Patch Documents", "Are you sure you want to apply this patch to all matching documents?")
                .ContinueWhenTrueInTheUIThread(() =>
                {
                    var request = new ScriptedPatchRequest { Script = patchModel.Script.CurrentSnapshot.Text };

                    switch (patchModel.PatchOn)
                    {
                        case PatchOnOptions.Document:
                            var commands = new ICommandData[1];
                            commands[0] = new ScriptedPatchCommandData
                            {
                                Patch = request,
                                Key = patchModel.SelectedItem
                            };

                            ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands);
                            break;

                        case PatchOnOptions.Collection:
                            ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(PatchModel.CollectionsIndex,
                                                                                                new IndexQuery { Query = "Tag:" + patchModel.SelectedItem },
                                                                                                request);
                            break;

                        case PatchOnOptions.Index:
                            ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(patchModel.SelectedItem, new IndexQuery() { Query = patchModel.QueryDoc.CurrentSnapshot.Text },
                                                                                                request);
                            break;
                    }
                });
		}
	}

	public enum PatchOnOptions
	{
		Document,
		Collection,
		Index
	}
}
