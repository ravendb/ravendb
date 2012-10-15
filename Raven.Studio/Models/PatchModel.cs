using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
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
using Raven.Studio.Messages;
using Notification = Raven.Studio.Messages.Notification;

namespace Raven.Studio.Models
{
	public class PatchModel : ViewModel, IAutoCompleteSuggestionProvider
	{
		private PatchOnOptions patchOn;

        private IEditorDocument originalDoc;
		private IEditorDocument newDoc;

	    private IEditorDocument queryDoc;
	    private Observable<string> selectedItem;

	    private static JsonSyntaxLanguageExtended JsonLanguage;
	    private static ISyntaxLanguage JScriptLanguage;
	    private static ISyntaxLanguage QueryLanguage;
		public string LoadedDoc { get; set; }
		public bool ShowDoc { get; set; }

	    static PatchModel()
        {
            JsonLanguage = new JsonSyntaxLanguageExtended();
            JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
            QueryLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
        }

        public PatchModel()
        {
			selectedItem = new Observable<string>();
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
	        ShowAfterPrompt = true;
            AvailableObjects = new ObservableCollection<string>();

            queryCollectionSource = new QueryDocumentsCollectionSource();
            QueryResults = new DocumentsModel(queryCollectionSource) { Header = "Matching Documents", MinimalHeader = true, HideItemContextMenu = true};
	        QueryResults.ItemSelection.SelectionChanged += (sender, args) =>
	        {
		        var firstOrDefault = QueryResults.ItemSelection.GetSelectedItems().FirstOrDefault();
				if (firstOrDefault != null)
				{
					OriginalDoc.SetText(firstOrDefault.Item.Document.ToJson().ToString());
					ShowBeforeAndAfterPrompt = false;
					HasSelection = true;
					OnPropertyChanged(() => HasSelection);
				}
				else
				{
					HasSelection = false;
					OnPropertyChanged(() => HasSelection);
					ClearBeforeAndAfter();
				}
	        };

	        selectedItem.PropertyChanged += (sender, args) =>
	        {
				if (PatchOn == PatchOnOptions.Document && string.IsNullOrWhiteSpace(SelectedItem) == false)
					ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(SelectedItem).
						ContinueOnSuccessInTheUIThread(doc =>
						{
							if (doc == null)
							{
								ClearBeforeAndAfter();
							}
							else
							{
								OriginalDoc.SetText(doc.ToJson().ToString());
								ShowBeforeAndAfterPrompt = false;
							}
						});
	        };
        }

        private QueryIndexAutoComplete queryIndexAutoComplete;
	    private QueryDocumentsCollectionSource queryCollectionSource;
		private bool showBeforeAndAfterPrompt;
		private bool showAfterPrompt;
		public bool HasSelection { get; set; }

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

		public string AfterPromptText
		{
			get { return "Press Test to try out your patch script"; }
		}

		public bool ShowBeforeAndAfterPrompt
	    {
	        get { return showBeforeAndAfterPrompt; }
            set
            {
                showBeforeAndAfterPrompt = value;
                OnPropertyChanged(() => ShowBeforeAndAfterPrompt);
				OnPropertyChanged(() => ShowAfterPrompt);
            }
	    }

		public bool ShowAfterPrompt
		{
			get
			{
				if (ShowBeforeAndAfterPrompt)
					return false;
				return showAfterPrompt;
			}
			set
			{
				showAfterPrompt = value;
				OnPropertyChanged(() => ShowAfterPrompt);
			}
		}

	    private void ClearBeforeAndAfter()
	    {
	        OriginalDoc.SetText("");
	        NewDoc.SetText("");
	        ShowBeforeAndAfterPrompt = true;
		    ShowAfterPrompt = true;
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
		    get { return selectedItem.Value; }
            set
            {
                selectedItem.Value = value;
                OnPropertyChanged(() => SelectedItem);
                UpdateQueryAutoComplete();
                UpdateCollectionSource();
                ClearBeforeAndAfter();
            }
		}

	    public void UpdateCollectionSource()
	    {
	        if (PatchOn == PatchOnOptions.Collection)
	        {
                QueryResults.SetChangesObservable(d => d.IndexChanges
					.Where(n => n.Name.Equals(PatchModel.CollectionsIndex, StringComparison.InvariantCulture))
					.Select(m => Unit.Default));

				if (string.IsNullOrWhiteSpace(SelectedItem) == false)
					queryCollectionSource.UpdateQuery(PatchModel.CollectionsIndex, new IndexQuery {Query = "Tag:" + SelectedItem});
	        }
            else if (PatchOn == PatchOnOptions.Index)
            {
                QueryResults.SetChangesObservable(d => d.IndexChanges
					.Where(n => n.Name.Equals(SelectedItem, StringComparison.InvariantCulture))
					.Select(m => Unit.Default));

				if (string.IsNullOrWhiteSpace(SelectedItem) == false)
					queryCollectionSource.UpdateQuery(SelectedItem, new IndexQuery { Query = QueryDoc.CurrentSnapshot.Text, SkipTransformResults = true, });
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
		public ICommand Patch { get { return new ExecutePatchCommand(this); } }
		public ICommand PatchSelected { get { return new PatchSelectedCommand(this); } }
		public ICommand Test { get { return new TestPatchCommand(this); } }
		public ICommand Save { get { return new SavePatchCommand(this); } }
		public ICommand Load { get { return new LoadPatchCommand(this); } }

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
	                        SelectedItem = AvailableObjects.FirstOrDefault();
                        });
                    break;
                case PatchOnOptions.Index:
                     ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexNamesAsync(0, 500)
                        .ContinueOnSuccessInTheUIThread(indexes =>
                        {
                            AvailableObjects.Clear();
                            AvailableObjects.AddRange(indexes.OrderBy(x => x));
							SelectedItem = AvailableObjects.FirstOrDefault();
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

		public void UpdateDoc(string name)
		{
			LoadedDoc = name;
			ShowDoc = true;
			OnPropertyChanged(() => LoadedDoc);
			OnPropertyChanged(() => ShowDoc);
			OnPropertyChanged(() => QueryDoc);
			OnPropertyChanged(() => Script);
			OnPropertyChanged(() => PatchOn);
			OnPropertyChanged(() => SelectedItem);
		}
	}

	public class LoadPatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public LoadPatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			AskUser.SelectItem("Load", "Choose saved patching to load",
			                                    () => ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession().Advanced.
				                                             LoadStartingWithAsync<PatchDocument>("Studio/Patch/").ContinueWith(
					                                             task =>
					                                             {
						                                             IList<string> objects = task.Result.Select(document => document.Id.Substring("Studio/Patch/".Length)).ToList();
						                                             return objects;
					                                             }))
				.ContinueOnSuccessInTheUIThread(result => ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession().
					                                          LoadAsync<PatchDocument>("Studio/Patch/" + result)
					                                          .ContinueOnSuccessInTheUIThread(patch =>
					                                          {
																  if (patch == null)
																	  ApplicationModel.Current.Notifications.Add(new Notification("Could not find Patch document named " + result, NotificationLevel.Error));
																  else
																  {
																	  patchModel.PatchOn = patch.PatchOnOption;
																	  patchModel.QueryDoc.SetText(patch.Query);
																	  patchModel.Script.SetText(patch.Script);
																	  patchModel.SelectedItem = patch.SelectedItem;
																	  patchModel.UpdateDoc(result);
																  }
					                                          }));
		}
	}

	public class SavePatchCommand : Command
	{
		private readonly PatchModel patchModel;

		public SavePatchCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			AskUser.QuestionAsync("Save", "Please enter a name").ContinueOnSuccessInTheUIThread(name =>
			{
				var doc = new PatchDocument
				{
					PatchOnOption = patchModel.PatchOn,
					Query = patchModel.QueryDoc.CurrentSnapshot.GetText(LineTerminator.Newline),
					Script = patchModel.Script.CurrentSnapshot.GetText(LineTerminator.Newline),
					SelectedItem = patchModel.SelectedItem,
					Id = "Studio/Patch/" + name
				};

				var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession();
				session.Store(doc);
				session.SaveChangesAsync().ContinueOnSuccessInTheUIThread(() => patchModel.UpdateDoc(name));
				//ApplicationModel.DatabaseCommands.PutAsync("Studio/Patch/" + name, new Guid(), RavenJObject.FromObject(doc), new RavenJObject());


			});
		}
	}

	public class PatchSelectedCommand : Command
	{
		private readonly PatchModel patchModel;

		public PatchSelectedCommand(PatchModel patchModel)
		{
			this.patchModel = patchModel;
		}

		public override void Execute(object parameter)
		{
			AskUser.ConfirmationAsync("Patch Documents", "Are you sure you want to apply this patch to all selected documents?")
				.ContinueWhenTrueInTheUIThread(() =>
				{
					var request = new ScriptedPatchRequest {Script = patchModel.Script.CurrentSnapshot.Text};
					var selectedItems = patchModel.QueryResults.ItemSelection.GetSelectedItems();
					var commands = new ICommandData[selectedItems.Count()];
					var counter = 0;

					foreach (var item in selectedItems)
					{
						commands[counter] = new ScriptedPatchCommandData
						{
							Patch = request,
							Key = item.Item.Id
						};

						counter++;
					}

					ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands).Catch();
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
			var commands = new ICommandData[1];

			switch (patchModel.PatchOn)
			{
				case PatchOnOptions.Document:
					ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(patchModel.SelectedItem).
						ContinueOnSuccessInTheUIThread(doc => patchModel.OriginalDoc.SetText(doc.ToJson().ToString()));

					commands[0] = new ScriptedPatchCommandData
					{
						Patch = request,
						Key = patchModel.SelectedItem,
						DebugMode = true
					};

					ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands)
						.ContinueOnSuccessInTheUIThread(batch => patchModel.NewDoc.SetText(batch[0].AdditionalData.ToString())).Catch();
					break;

				case PatchOnOptions.Collection:
				case PatchOnOptions.Index:
			        var selectedItem = patchModel.QueryResults.ItemSelection.GetSelectedItems().FirstOrDefault();
                    if (selectedItem == null || !selectedItem.IsRealized)
                    {
                        return;
                    }

                    patchModel.OriginalDoc.SetText(selectedItem.Item.Document.ToJson().ToString());
			        var docId = selectedItem.Item.Document.Key;

					commands[0] = new ScriptedPatchCommandData
					{
						Patch = request,
						Key = docId,
						DebugMode = true
					};
					
					ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands)
						.ContinueOnSuccessInTheUIThread(batch => patchModel.NewDoc.SetText(batch[0].AdditionalData.ToString())).Catch();
					break;
			}

		    patchModel.ShowBeforeAndAfterPrompt = false;
			patchModel.ShowAfterPrompt = false;
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

                            ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands).Catch();
                            break;

                        case PatchOnOptions.Collection:
                            ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(PatchModel.CollectionsIndex,
                                                                                                new IndexQuery { Query = "Tag:" + patchModel.SelectedItem }, request)
																								.ContinueOnSuccessInTheUIThread(() => patchModel.UpdateCollectionSource())
																								.Catch();
                            break;

                        case PatchOnOptions.Index:
                            ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(patchModel.SelectedItem, new IndexQuery() { Query = patchModel.QueryDoc.CurrentSnapshot.Text },
                                                                                                request)
																								.ContinueOnSuccessInTheUIThread(() => patchModel.UpdateCollectionSource())
																								.Catch();
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

	public class PatchDocument
	{
		public PatchOnOptions PatchOnOption { get; set; }
		public string Query { get; set; }
		public string Script { get; set; }
		public string SelectedItem { get; set; }
		public string Id { get; set; }
	}
}
