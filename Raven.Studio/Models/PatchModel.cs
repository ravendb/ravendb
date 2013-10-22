using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Studio.Behaviors;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Features.Patch;
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

	    private string selectedItem;

	    private static JsonSyntaxLanguageExtended JsonLanguage;
	    private static ISyntaxLanguage JScriptLanguage;
	    private static ISyntaxLanguage QueryLanguage;
		public string LoadedDoc { get; set; }
		public bool ShowDoc { get; set; }
        private ObservableCollection<JsonDocument> recentDocuments = new ObservableCollection<JsonDocument>();
        public Observable<string> PatchScriptErrorMessage { get; private set; }
        public Observable<bool> IsErrorVisible { get; private set; } 

	    static PatchModel()
        {
            JsonLanguage = new JsonSyntaxLanguageExtended();
            JScriptLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("JScript.langdef");
            QueryLanguage = SyntaxEditorHelper.LoadLanguageDefinitionFromResourceStream("RavenQuery.langdef");
        }

        public PatchModel()
        {
            Values = new ObservableCollection<PatchValue>();
			InProcess = new Observable<bool>();

            OriginalDoc = new EditorDocument
            {
                Language = JsonLanguage,
                IsReadOnly = true,
            };

            NewDoc = new EditorDocument
            {
                Language = JsonLanguage,
                IsReadOnly = true,
            };

            Script = new EditorDocument
            {
                Language = JScriptLanguage
            };

            Script.Language.RegisterService(new PatchScriptIntelliPromptProvider(Values, recentDocuments));
            Script.Language.RegisterService<IEditorDocumentTextChangeEventSink>(new AutoCompletionTrigger());

            QueryDoc = new EditorDocument
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
                    UpdateBeforeDocument(firstOrDefault.Item.Document);
					HasSelection = true;
				}
				else
				{
					HasSelection = false;
					ClearBeforeAndAfter();
				}
	        };

            QueryResults.RecentDocumentsChanged += delegate
            {
                recentDocuments.Clear();
                recentDocuments.AddRange(QueryResults.GetMostRecentDocuments().Where(d => d.Document != null).Take(5).Select(d => d.Document));
            };

            PatchScriptErrorMessage = new Observable<string>();
            IsErrorVisible = new Observable<bool>();
        }

        public void ClearQueryError()
        {
            PatchScriptErrorMessage.Value = string.Empty;
            IsErrorVisible.Value = false;
        }

	    private void UpdateSpecificDocument()
	    {
	        if (PatchOn == PatchOnOptions.Document && string.IsNullOrWhiteSpace(SelectedItem) == false)
	        {
	            recentDocuments.Clear();
	            ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(SelectedItem).
	                ContinueOnSuccessInTheUIThread(doc =>
	                {
	                    if (doc == null)
	                    {
	                        ClearBeforeAndAfter();
	                    }
	                    else
	                    {
	                        UpdateBeforeDocument(doc);
                            recentDocuments.Add(doc);
	                    }
	                });
	        }
	    }

	    private void UpdateBeforeDocument(JsonDocument doc)
	    {
            recentDocuments.Add(doc);
	        OriginalDoc.SetText(doc.ToJson().ToString());
	        NewDoc.SetText("");
	        ShowBeforeAndAfterPrompt = false;
	        ShowAfterPrompt = true;
	    }

	    private QueryIndexAutoComplete queryIndexAutoComplete;
	    private QueryDocumentsCollectionSource queryCollectionSource;
		private bool showBeforeAndAfterPrompt;
		private bool showAfterPrompt;
	    private bool hasSelection;
	    public bool HasSelection
	    {
	        get { return hasSelection; }
	        private set
	        {
	            hasSelection = value; OnPropertyChanged(() => HasSelection);
	        }
	    }

	    protected QueryIndexAutoComplete QueryIndexAutoComplete
        {
            get { return queryIndexAutoComplete; }
            set
            {
                queryIndexAutoComplete = value;
                QueryDoc.Language.UnregisterService<ICompletionProvider>();
                QueryDoc.Language.RegisterService(value.CompletionProvider);
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
				if(KeepSelectedItem == false)
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
		    get { return selectedItem; }
            set
            {
				if (value == null && KeepSelectedItem)
					return;
                selectedItem = value;
                OnPropertyChanged(() => SelectedItem);
                recentDocuments.Clear();
                UpdateQueryAutoComplete();
                UpdateCollectionSource();
                UpdateSpecificDocument();
                ClearBeforeAndAfter();
            }
		}

	    public void UpdateCollectionSource()
	    {
            recentDocuments.Clear();
		    NewDoc.SetText("");
		    ShowAfterPrompt = true;
	        if (PatchOn == PatchOnOptions.Collection)
	        {
                QueryResults.SetChangesObservable(d => d.IndexChanges
					.Where(n => n.Name.Equals(CollectionsIndex, StringComparison.InvariantCulture))
					.Select(m => Unit.Default));

				if (string.IsNullOrWhiteSpace(SelectedItem) == false)
					queryCollectionSource.UpdateQuery(CollectionsIndex, new IndexQuery {Query = "Tag:" + SelectedItem});
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
		public ObservableCollection<PatchValue> Values { get; set; }
		public PatchValue SelectedValue { get; set; }
		public bool KeepSelectedItem { private get; set; }

		public const string CollectionsIndex = "Raven/DocumentsByEntityName";
		public ICommand CopyErrorTextToClipboard { get { return new ActionCommand(() => Clipboard.SetText(PatchScriptErrorMessage.Value)); } }
		public ICommand Patch { get { return new ExecutePatchCommand(this); } }
		public ICommand PatchSelected { get { return new PatchSelectedCommand(this); } }
		public ICommand Test { get { return new TestPatchCommand(this); } }
		public ICommand Save { get { return new SavePatchCommand(this); } }
		public ICommand Load { get { return new LoadPatchCommand(this); } }
		public ICommand AddValue{get{return new ActionCommand(() =>
		{
			Values.Add(new PatchValue(Values));
			OnPropertyChanged(() => Values);
		});}}
		public ICommand DeleteValue
		{
			get
			{
				return new ActionCommand(() =>
				{
					if (SelectedValue == null)
						return;
					Values.Remove(SelectedValue);
					OnPropertyChanged(() => Values);
				});
			}
		}

		public Observable<bool> InProcess { get; private set; }

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
						.ContinueWith(t => (IList<object>) t.Result.Where(s => s.StartsWith(enteredText, StringComparison.OrdinalIgnoreCase)).Cast<object>().ToList());

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
							if(KeepSelectedItem == false)
								SelectedItem = AvailableObjects.FirstOrDefault();
							else
								KeepSelectedItem = false;								

							OnPropertyChanged(() => SelectedItem);
                        });
                    break;
                case PatchOnOptions.Index:
                     ApplicationModel.Database.Value.AsyncDatabaseCommands.GetIndexNamesAsync(0, 500)
                        .ContinueOnSuccessInTheUIThread(indexes =>
                        {
                            AvailableObjects.Clear();
                            AvailableObjects.AddRange(indexes.OrderBy(x => x));
							if (KeepSelectedItem == false)
								SelectedItem = AvailableObjects.FirstOrDefault();
							else
								KeepSelectedItem = false;

							OnPropertyChanged(() => SelectedItem);
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
                .Subscribe(e => UpdateCollectionSource());
        }


        public void HandlePatchError(AggregateException exception)
        {
			var message = exception.TryReadResponseIfWebException().Result.TryReadErrorPropertyFromJson();
			PatchScriptErrorMessage.Value = string.IsNullOrEmpty(message) ? exception.ExtractSingleInnerException().Message : message;
            IsErrorVisible.Value = true;
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
			OnPropertyChanged(() => Values);
			OnPropertyChanged(() => SelectedItem);
		}

		public Dictionary<string, object> GetValues()
		{
			var values = new Dictionary<string, object>();

			foreach (var patchValue in Values)
			{
				if(values.ContainsKey(patchValue.Key))
				{
					MessageBox.Show("You Can not have more then one value for each key. (The key " + patchValue.Key + " apprears more then once");
					return null;
				}
				int integer;
				if(int.TryParse(patchValue.Value, out integer))
				{
					values.Add(patchValue.Key, integer);
					continue;
				}

				long longNum;
				if(long.TryParse(patchValue.Value, out longNum))
				{
					values.Add(patchValue.Key, longNum);
					continue;
				}

				decimal decimalNum;
				if (decimal.TryParse(patchValue.Value, out decimalNum))
				{
					values.Add(patchValue.Key, decimalNum);
					continue;
				}

				bool boolean;
				if (bool.TryParse(patchValue.Value, out boolean))
				{
					values.Add(patchValue.Key, boolean);
					continue;
				}

				values.Add(patchValue.Key, patchValue.Value);
			}

			return values;
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
			var dbName = ApplicationModel.Database.Value.Name;
			if (dbName == Constants.SystemDatabase)
				dbName = null;
			AskUser.SelectItem("Load", "Choose saved patching to load",
												() => ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(dbName).Advanced.
				                                             LoadStartingWithAsync<PatchDocument>("Studio/Patch/").ContinueWith(
					                                             task =>
					                                             {
						                                             IList<string> objects = task.Result.Select(document => document.Id.Substring("Studio/Patch/".Length)).ToList();
						                                             return objects;
					                                             }))
				.ContinueOnSuccessInTheUIThread(result => ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(dbName).
					                                          LoadAsync<PatchDocument>("Studio/Patch/" + result)
					                                          .ContinueOnSuccessInTheUIThread(patch =>
					                                          {
																  if (patch == null)
																	  ApplicationModel.Current.Notifications.Add(new Notification("Could not find Patch document named " + result, NotificationLevel.Error));
																  else
																  {
																	  patchModel.KeepSelectedItem = true;
																	  patchModel.SelectedItem = patch.SelectedItem;
																	  patchModel.PatchOn = patch.PatchOnOption;
																	  patchModel.QueryDoc.SetText(patch.Query);
																	  patchModel.Script.SetText(patch.Script);
																	  patchModel.Values = new ObservableCollection<PatchValue>(patch.Values);
																	  patchModel.UpdateDoc(result);

                                                                      patchModel.ClearQueryError();
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
			AskUser.QuestionAsync("Save", "Please enter a name").ContinueOnSuccessInTheUIThread(async name =>
			{
				var doc = new PatchDocument
				{
					PatchOnOption = patchModel.PatchOn,
					Query = patchModel.QueryDoc.CurrentSnapshot.GetText(LineTerminator.Newline),
					Script = patchModel.Script.CurrentSnapshot.GetText(LineTerminator.Newline),
					SelectedItem = patchModel.SelectedItem,
					Id = "Studio/Patch/" + name,
					Values = patchModel.Values.ToList()
				};
				var dbName = ApplicationModel.Database.Value.Name;
				if (dbName == Constants.SystemDatabase)
					dbName = null;

				var session = ApplicationModel.Current.Server.Value.DocumentStore.OpenAsyncSession(dbName);
				await session.StoreAsync(doc);
				await session.SaveChangesAsync().ContinueOnSuccessInTheUIThread(() => patchModel.UpdateDoc(name));
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
            patchModel.ClearQueryError();

			AskUser.ConfirmationAsync("Patch Documents", "Are you sure you want to apply this patch to all selected documents?")
				.ContinueWhenTrueInTheUIThread(() =>
				{
					var values = patchModel.GetValues();
					if (values == null)
						return;
					var request = new ScriptedPatchRequest {Script = patchModel.Script.CurrentSnapshot.Text, Values = values};
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

					ApplicationModel.Database.Value.AsyncDatabaseCommands
						.BatchAsync(commands)
                         .ContinueOnUIThread(t => { if (t.IsFaulted) patchModel.HandlePatchError(t.Exception); })
						.ContinueOnSuccessInTheUIThread(() => ApplicationModel.Database.Value
							.AsyncDatabaseCommands
							.GetAsync(patchModel.SelectedItem)
							.ContinueOnSuccessInTheUIThread(doc =>
							{
								if (doc != null)
								{
									patchModel.OriginalDoc.SetText(doc.ToJson().ToString());
									patchModel.NewDoc.SetText("");
									patchModel.ShowAfterPrompt = true;
								}
								else
								{
									patchModel.OriginalDoc.SetText("");
									patchModel.NewDoc.SetText("");
									patchModel.ShowAfterPrompt = true;
									patchModel.ShowBeforeAndAfterPrompt = true;
								}
							}));
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
            patchModel.ClearQueryError();

			var values = patchModel.GetValues();
			if (values == null)
				return;
			var request = new ScriptedPatchRequest {Script = patchModel.Script.CurrentSnapshot.Text, Values = values};
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

					break;
			}

			patchModel.InProcess.Value = true;

			ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands)
				.ContinueOnSuccessInTheUIThread(batch => patchModel.NewDoc.SetText(batch[0].AdditionalData.ToString()))
				.ContinueOnUIThread(t => { if (t.IsFaulted) patchModel.HandlePatchError(t.Exception); })
				.Finally(() => patchModel.InProcess.Value = false);

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
                    patchModel.ClearQueryError();

					var values = patchModel.GetValues();
					if (values == null)
						return;
                    var request = new ScriptedPatchRequest { Script = patchModel.Script.CurrentSnapshot.Text, Values = values};
					patchModel.InProcess.Value = true;

                    switch (patchModel.PatchOn)
                    {
                        case PatchOnOptions.Document:
                            var commands = new ICommandData[1];
                            commands[0] = new ScriptedPatchCommandData
                            {
                                Patch = request,
                                Key = patchModel.SelectedItem
                            };

		                    ApplicationModel.Database.Value.AsyncDatabaseCommands.BatchAsync(commands)
			                    .ContinueOnUIThread(t => { if (t.IsFaulted) patchModel.HandlePatchError(t.Exception); })
			                    .ContinueOnSuccessInTheUIThread(
				                    () => ApplicationModel.Database.Value.AsyncDatabaseCommands.GetAsync(patchModel.SelectedItem).
					                          ContinueOnSuccessInTheUIThread(doc =>
					                          {
						                          patchModel.OriginalDoc.SetText(doc.ToJson().ToString());
						                          patchModel.NewDoc.SetText("");
						                          patchModel.ShowAfterPrompt = true;
					                          }))
			                    .Finally(() => patchModel.InProcess.Value = false);
                            break;

						case PatchOnOptions.Collection:
							ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(PatchModel.CollectionsIndex,
																								new IndexQuery { Query = "Tag:" + patchModel.SelectedItem }, request)
																								.ContinueOnSuccessInTheUIThread(() => patchModel.UpdateCollectionSource())
																								 .ContinueOnUIThread(t => { if (t.IsFaulted) patchModel.HandlePatchError(t.Exception); })
																								 .Finally(() => patchModel.InProcess.Value = false);
							break;

						case PatchOnOptions.Index:
							ApplicationModel.Database.Value.AsyncDatabaseCommands.UpdateByIndex(patchModel.SelectedItem, new IndexQuery { Query = patchModel.QueryDoc.CurrentSnapshot.Text },
																								request)
																								.ContinueOnSuccessInTheUIThread(() => patchModel.UpdateCollectionSource())
																								 .ContinueOnUIThread(t => { if (t.IsFaulted) patchModel.HandlePatchError(t.Exception); })
																								 .Finally(() => patchModel.InProcess.Value = false);
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
		public List<PatchValue> Values { get; set; } 
	}

	public class PatchValue
	{
		private readonly ObservableCollection<PatchValue> values;
		private string key;

		public PatchValue(ObservableCollection<PatchValue> values)
		{
			this.values = values;
		}

		public string Key
		{
			get { return key; }
			set
			{
				if (value == key)
					return;
				if(values != null && values.Any(patchValue => patchValue.Key == value))
					MessageBox.Show("You already have an item with the key:  " + value, "Duplicate parameter name detected", MessageBoxButton.OK);
				else
					key = value;
			}
		}
		public string Value { get; set; }
	}
}
