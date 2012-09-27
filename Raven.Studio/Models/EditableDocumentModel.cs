using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Input;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Parsing.LLParser;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using Microsoft.Expression.Interactivity.Core;
using Raven.Abstractions.Json;
using Raven.Abstractions.Logging;
using Raven.Client.Exceptions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Input;
using Raven.Studio.Features.JsonEditor;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;
using Raven.Abstractions.Extensions;
using Raven.Studio.Extensions;

namespace Raven.Studio.Models
{
	public class EditableDocumentModel : PageViewModel
	{
		private readonly Observable<JsonDocument> document;
		private int currentIndex;
		private int totalItems;
		public string DocumentKey { get; private set; }
		private readonly string currentDatabase;
		private DocumentNavigator navigator;
		private ICommand navigateNext;
		private ICommand navigatePrevious;
		private string urlForFirst;
		private string urlForPrevious;
		private string urlForNext;
		private string urlForLast;
		private DocumentSection dataSection;
		private DocumentSection metaDataSection;
		private ICommand deleteCommand;
		private ICommand navigateFirst;
		private ICommand navigateLast;
		private string documentSize;
		private static JsonSyntaxLanguageExtended JsonLanguage;
		private bool isShowingErrors;
		private bool hasUnsavedChanges;

		static EditableDocumentModel()
		{
			InitialiseOutliningModes();
			JsonLanguage = new JsonSyntaxLanguageExtended();
		}

		private static void InitialiseOutliningModes()
		{
			OutliningModes = (new List<DocumentOutliningMode>()
								  {
									  new DocumentOutliningMode("Disabled")
										  {Applicator = document => document.OutliningMode = OutliningMode.None},
									  new DocumentOutliningMode("Enabled")
										  {Applicator = document =>
										  {
                                              document.OutliningMode = OutliningMode.None;
										      document.OutliningMode = OutliningMode.Automatic;
										  }},
									  new DocumentOutliningMode("Auto-Collapse Collections")
										  {
											  Applicator = document =>
															   {
                                                                   // for some reason (possibly bug in Syntax Editor),
                                                                   // outlining is not correctly updated when the page is first shown
                                                                   // unless we turn it off first
                                                                   document.OutliningMode = OutliningMode.None;
																   document.OutliningMode = OutliningMode.Automatic;
																   document.OutliningManager.EnsureCollapsed();
															   }
										  },
								  }).AsReadOnly();
		}

		public EditableDocumentModel()
		{
			ModelUrl = "/edit";

			dataSection = new DocumentSection() { Name = "Data", Document = new EditorDocument() { Language = JsonLanguage, TabSize = 2 } };
			metaDataSection = new DocumentSection() { Name = "Metadata", Document = new EditorDocument() { Language = JsonLanguage, TabSize = 2 } };
			DocumentSections = new List<DocumentSection>() { dataSection, metaDataSection };
			CurrentSection = dataSection;

			References = new BindableCollection<LinkModel>(model => model.Title);
			Related = new BindableCollection<LinkModel>(model => model.Title);
			DocumentErrors = new ObservableCollection<DocumentError>();

			SearchEnabled = false;

			document = new Observable<JsonDocument>();
			document.PropertyChanged += (sender, args) => UpdateFromDocument();
			documentIdManager = JsonDataDocument.Properties.GetOrCreateSingleton(() => new DocumentReferencedIdManager());

			InitialiseDocument();

			ParentPathSegments = new ObservableCollection<PathSegment>()
									 {
										 new PathSegment() { Name="Documents", Url = "/documents"}
									 };

			currentDatabase = Database.Value.Name;

			dataSection.Document.ObserveTextChanged()
				.Merge(metaDataSection.Document.ObserveTextChanged())
				.Do(_ => HasUnsavedChanges = true)
				.Throttle(TimeSpan.FromSeconds(1))
				.ObserveOnDispatcher()
				.Subscribe(e => HandleDocumentChanged());
		}

		private void HandleDocumentChanged()
		{
			UpdateErrors();
			UpdateDocumentSize();
			UpdateReferences();
		}

		private void UpdateErrors()
		{
			DocumentErrors.Clear();

			AddErrors(dataSection);
			AddErrors(metaDataSection);
		}

		private void AddErrors(DocumentSection section)
		{
			var parseData = section.Document.ParseData as ILLParseData;
			if (parseData == null)
			{
				return;
			}

			foreach (var parseError in parseData.Errors)
			{
				DocumentErrors.Add(new DocumentError() { Section = section, ParseError = parseError });
			}
		}

		private void InitialiseDocument()
		{
			document.Value = new JsonDocument
								{
									DataAsJson = { { "Name", "..." } },
									Etag = Guid.Empty
								};
		}

		private DocumentNavigator Navigator
		{
			get { return navigator; }
			set
			{
				navigator = value;
				OnPropertyChanged(() => Navigator);
				OnPropertyChanged(() => CanNavigate);
			}
		}

		public ICommand NavigateToNext
		{
			get
			{
				return navigateNext ??
					   (navigateNext = new ActionCommand(() => HandleNavigation(urlForNext)));
			}
		}

		public ICommand NavigateToPrevious
		{
			get
			{
				return navigatePrevious ??
					   (navigatePrevious = new ActionCommand(() => HandleNavigation(urlForPrevious)));
			}
		}

		public ICommand NavigateToFirst
		{
			get
			{
				return navigateFirst ??
					   (navigateFirst = new ActionCommand(() => HandleNavigation(urlForFirst)));
			}
		}

		public ICommand NavigateToLast
		{
			get
			{
				return navigateLast ??
					   (navigateLast = new ActionCommand(() => HandleNavigation(urlForLast)));
			}
		}

		public ICommand ToggleExpansion
		{
			get { return toggleExpansion ?? (toggleExpansion = new ActionCommand(HandleToggleExpansion)); }
		}

		public DocumentOutliningMode SelectedOutliningMode
		{
			get { return outliningMode; }
			set
			{
				outliningMode = value;
				OnPropertyChanged(() => SelectedOutliningMode);

				StoreOutliningMode();
				ApplyOutliningMode();
			}
		}

		private void StoreOutliningMode()
		{
			Settings.Instance.DocumentOutliningMode = SelectedOutliningMode.Name;
		}

		private void ApplyOutliningMode()
		{
			if (outliningMode != null)
			{
				foreach (var document in DocumentSections.Select(s => s.Document))
				{
					outliningMode.Applicator(document);
				}
			}
		}

		private void HandleToggleExpansion()
		{
			if (CurrentSection.Document == null)
			{
				return;
			}

			CurrentSection.Document.OutliningMode = OutliningMode.Automatic;
			CurrentSection.Document.OutliningManager.ToggleAllOutliningExpansion();
		}

		private void HandleNavigation(string url)
		{
			if (!string.IsNullOrEmpty(url))
			{
				UrlUtil.Navigate(url);
			}
		}

		public override void LoadModelParameters(string parameters)
		{
			var url = new UrlParser(UrlUtil.Url);
			documentIdManager.Clear();

			if (url.GetQueryParam("mode") == "new")
			{
				Mode = DocumentMode.New;
				InitialiseDocument();
				Navigator = null;
				CurrentIndex = 0;
				TotalItems = 0;
				SetCurrentDocumentKey(null);
				ParentPathSegments.Clear();
				ParentPathSegments.Add(new PathSegment() { Name = "Documents", Url = "/documents" });
				return;
			}

			var database = url.GetQueryParam("databaseid");
			if(database != null)
			{
				EditDatabaseDocument(database);
				return;
			}

			Navigator = DocumentNavigator.FromUrl(url);

			Navigator.GetDocument().ContinueOnSuccessInTheUIThread(
				result =>
				{
					if (result.Document == null)
					{
						HandleDocumentNotFound();
						return;
					}

					if (string.IsNullOrEmpty(result.Document.Key))
					{
						Mode = DocumentMode.Projection;
						LocalId = Guid.NewGuid().ToString();
					}
					else
					{
						Mode = DocumentMode.DocumentWithId;
						LocalId = result.Document.Key;
						SetCurrentDocumentKey(result.Document.Key);
					}

					urlForFirst = result.UrlForFirst;
					urlForPrevious = result.UrlForPrevious;
					urlForLast = result.UrlForLast;
					urlForNext = result.UrlForNext;

					IsLoaded = true;
					document.Value = result.Document;
					CurrentIndex = (int)result.Index;
					TotalItems = (int)result.TotalDocuments;

					ParentPathSegments.Clear();
					ParentPathSegments.AddRange(result.ParentPath);

					WhenParsingComplete(dataSection.Document)
						.ContinueOnUIThread(t => ApplyOutliningMode());

					HandleDocumentChanged();
				})
				.Catch(exception =>
				{
					var conflictExeption = exception.GetBaseException() as ConflictException;

					if (conflictExeption != null)
					{
						ApplicationModel.Current.Server.Value.SelectedDatabase.Value
							.AsyncDatabaseCommands
							.GetAsync(conflictExeption.ConflictedVersionIds, null)
							.ContinueOnSuccessInTheUIThread(doc =>
							{
								var docs = new List<RavenJObject>();
								var metadatas = new List<RavenJObject>();
								foreach (var result in doc.Results)
								{
									metadatas.Add(result.Value<RavenJObject>("@metadata"));
									result.Remove("@metadata");
									docs.Add(result);
								}

								ClearMetadatas(metadatas);

								var docsConflictsResolver = new ConflictsResolver(docs.ToArray());
								var metadataConflictsResolver = new ConflictsResolver(metadatas.ToArray());

								Key = url.GetQueryParam("id");
								DocumentKey = Key;
								OnPropertyChanged(() => DisplayId);
								Etag = conflictExeption.Etag;
	
								ResolvingConflict = true;

								dataSection.Document.DeleteText(TextChangeTypes.Custom, 0, dataSection.Document.CurrentSnapshot.Length);
								dataSection.Document.AppendText(TextChangeTypes.Custom, docsConflictsResolver.Resolve());

								metaDataSection.Document.DeleteText(TextChangeTypes.Custom, 0, metaDataSection.Document.CurrentSnapshot.Length);
								metaDataSection.Document.AppendText(TextChangeTypes.Custom, metadataConflictsResolver.Resolve());

								OnPropertyChanged(() => dataSection);
								OnPropertyChanged(() => document);
							});
						return true;
					}

					return false;
				});
		}

		public bool EditingDatabase { get; set; }

		private void EditDatabaseDocument(string database)
		{
			ApplicationModel.Current.Server.Value.DocumentStore
				.AsyncDatabaseCommands
				.ForDefaultDatabase()
				.CreateRequest("/admin/databases/" + database, "GET")
				.ReadResponseJsonAsync()
				.ContinueOnSuccessInTheUIThread(result =>
				{
					var doc = result as RavenJObject;
					

					if (doc == null)
						return;
					var meta = doc.Value<RavenJObject>("@metadata");
					if(meta == null)
						meta = new RavenJObject();
					doc.Remove("@metadata");
					doc.Remove("Id");

					EditingDatabase = true;
					OnPropertyChanged(() => EditingDatabase);
					Key = database;
					dataSection.Document.DeleteText(TextChangeTypes.Custom, 0, dataSection.Document.CurrentSnapshot.Length);
					dataSection.Document.AppendText(TextChangeTypes.Custom, doc.ToString());
					metaDataSection.Document.DeleteText(TextChangeTypes.Custom, 0, metaDataSection.Document.CurrentSnapshot.Length);
					metaDataSection.Document.AppendText(TextChangeTypes.Custom, meta.ToString());
				});
		}

		private static void ClearMetadatas(IEnumerable<RavenJObject> metadatas)
		{
			foreach (var meta in metadatas)
			{
				var keysToRemove = new List<string>();
				foreach (var item in meta)
				{
					if (item.Key.StartsWith("Raven-Replication-") || item.Key == "@id" || item.Key == "Last-Modified" ||
					    item.Key == "@etag" || item.Key == "Non-Authoritative-Information")
						keysToRemove.Add(item.Key);
				}
				foreach (var key in keysToRemove)
				{
					meta.Remove(key);
				}
			}
		}

		public bool ResolvingConflict
		{
			get { return resolvingConflict; }
			set
			{
				resolvingConflict = value;
				OnPropertyChanged(() => ResolvingConflict);
			}
		}

		private void HandleDocumentNotFound()
		{
			Notification notification;
			if (Mode == DocumentMode.Projection)
				notification = new Notification("Could not parse projection correctly", NotificationLevel.Error);
			else
				notification = new Notification(string.Format("Could not find '{0}' document", Key),
												NotificationLevel.Warning);
			ApplicationModel.Current.AddNotification(notification);
			UrlUtil.Navigate("/documents");
		}

		public int CurrentItemNumber
		{
			get { return CurrentIndex + 1; }
		}

		private int CurrentIndex
		{
			get { return currentIndex; }
			set
			{
				currentIndex = value;
				OnPropertyChanged(() => CurrentItemNumber);
				OnPropertyChanged(() => HasPrevious);
				OnPropertyChanged(() => HasNext);
				OnPropertyChanged(() => CanNavigate);
			}
		}

		public int TotalItems
		{
			get { return totalItems; }
			set
			{
				totalItems = value;
				OnPropertyChanged(() => TotalItems);
				OnPropertyChanged(() => HasPrevious);
				OnPropertyChanged(() => HasNext);
				OnPropertyChanged(() => CanNavigate);
			}
		}

		public bool HasPrevious
		{
			get { return !string.IsNullOrEmpty(urlForPrevious); }
		}

		public bool HasNext
		{
			get { return !string.IsNullOrEmpty(urlForNext); }
		}

		public bool CanNavigate
		{
			get { return Navigator != null && (HasNext || HasPrevious); }
		}

		public bool IsShowingErrors
		{
			get { return isShowingErrors; }
			set
			{
				isShowingErrors = value;
				OnPropertyChanged(() => IsShowingErrors);
			}
		}

		public ObservableCollection<PathSegment> ParentPathSegments { get; private set; }
		public ObservableCollection<DocumentError> DocumentErrors { get; private set; }

		public IList<DocumentSection> DocumentSections { get; private set; }

		private DocumentSection currentSection;

		public DocumentSection CurrentSection
		{
			get { return currentSection; }
			set
			{
				currentSection = value;
				OnPropertyChanged(() => CurrentSection);
			}
		}

		public void SetCurrentDocumentKey(string docId)
		{
			if (docId != null)
			{
				Mode = DocumentMode.DocumentWithId;
			}
			else
			{
				Mode = DocumentMode.New;
			}

			DocumentKey = Key = docId;
		}

		private void PutDocumentKeyInUrl(string docId, bool dontOpenNewTab)
		{
			if (docId != null && DocumentKey != docId)
				UrlUtil.Navigate("/edit?id=" + docId, dontOpenNewTab);
		}

		private void UpdateFromDocument()
		{
			var newdoc = document.Value;
			RemoveNonDisplayedMetadata(newdoc.Metadata);
			UpdateMetadata(newdoc.Metadata);

			JsonData = newdoc.DataAsJson.ToString(Formatting.Indented);

			HasUnsavedChanges = false;
			UpdateRelated();
			OnEverythingChanged();
		}

		private void RemoveNonDisplayedMetadata(RavenJObject metaData)
		{
			metaData.Remove("@etag");
			metaData.Remove("@id");
		}

		private void UpdateMetadata(RavenJObject metadataAsJson)
		{
			metadata = metadataAsJson.ToDictionary(x => x.Key, x =>
															   {
																   if (x.Value.Type == JTokenType.String)
																	   return x.Value.Value<string>();
																   return x.Value.ToString(Formatting.None);
															   });
			OnPropertyChanged(() => Metadata);
			JsonMetadata = metadataAsJson.ToString(Formatting.Indented);
		}



		public BindableCollection<LinkModel> References { get; private set; }
		public BindableCollection<LinkModel> Related { get; private set; }

		private bool searchEnabled;
		public bool SearchEnabled
		{
			get { return searchEnabled; }
			set
			{
				searchEnabled = value;
				OnPropertyChanged(() => SearchEnabled);
			}
		}

		private string localId;
		public string LocalId
		{
			get { return localId; }
			set
			{
				localId = value;
				OnPropertyChanged(() => LocalId);
				OnPropertyChanged(() => CurrentItemNumber);
			}
		}

		public string DisplayId
		{
			get
			{
				if (Mode == DocumentMode.Projection)
					return "Projection";
				if (Mode == DocumentMode.New)
					return "New Document";
				return DocumentKey;
			}
		}

		private DocumentMode mode = DocumentMode.NotInitializedYet;
		public DocumentMode Mode
		{
			get { return mode; }
			set
			{
				mode = value;
				OnPropertyChanged(() => Mode);
				OnPropertyChanged(() => DisplayId);
			}
		}

		public IEditorDocument JsonDataDocument
		{
			get { return dataSection.Document; }
		}

		public IEditorDocument MetaDataDocument
		{
			get { return metaDataSection.Document; }
		}

		protected string JsonData
		{
			get { return JsonDataDocument.CurrentSnapshot.Text; }
			set { JsonDataDocument.SetText(value); }
		}

		protected string JsonMetadata
		{
			get { return MetaDataDocument.CurrentSnapshot.Text; }
			set { MetaDataDocument.SetText(value); }
		}

		public string DocumentSize
		{
			get { return documentSize; }
			private set
			{
				documentSize = value;
				OnPropertyChanged(() => DocumentSize);
			}
		}

		private void UpdateDocumentSize()
		{
			double byteCount = Encoding.UTF8.GetByteCount(JsonDataDocument.CurrentSnapshot.Text)
				+ Encoding.UTF8.GetByteCount(MetaDataDocument.CurrentSnapshot.Text);

			string sizeTerm = "Bytes";
			if (byteCount >= 1024 * 1024)
			{
				sizeTerm = "MBytes";
				byteCount = byteCount / (1024 * 1024);
			}
			else if (byteCount >= 1024)
			{
				sizeTerm = "KBytes";
				byteCount = byteCount / 1024;
			}

			DocumentSize = string.Format("Content-Length: {0:#,#.##;;0} {1}", byteCount, sizeTerm);
		}

		private void UpdateReferences()
		{
			if (Seperator == null)
				return;

			// Note: if this proves to be too slow with large documents, we can potential optimise the finding 
			// of references by only considering the parts of the AST which occur after the Offset at which the text change began
			// (we can find this by getting hold of the TextSnapshotChangedEventArgs)
			var potentialReferences = FindPotentialReferences(JsonDataDocument).ToList();
			var newReferences = potentialReferences.Where(id => documentIdManager.NeedsChecking(id)).ToArray();

			if (newReferences.Any())
			{
				ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetAsync(
					newReferences, null, metadataOnly: true)
					.ContinueOnSuccessInTheUIThread(results =>
					{
						var ids =
							results.Results.Where(r => r != null).Select(
								r => r["@metadata"].SelectToken("@id").ToString()).ToList();

						documentIdManager.AddKnownIds(ids);
						documentIdManager.AddKnownInvalidIds(newReferences.Except(ids));

						UpdateListWithKnownIds(potentialReferences);
					});
			}
			else
			{
				UpdateListWithKnownIds(potentialReferences);
			}

		}

		private void UpdateListWithKnownIds(IEnumerable<string> potentialReferences)
		{
			var referenceModels = potentialReferences.Where(id => documentIdManager.IsId(id))
				.Select(key => new LinkModel
				{
					Title = key,
					HRef = "/Edit?id=" + key
				})
				.ToArray();

			References.Match(referenceModels);
		}

		private IEnumerable<string> FindPotentialReferences(ICodeDocument document)
		{
			var stringValueNodes = document.FindAllStringValueNodes();
			return stringValueNodes.Select(n => n.Text).Distinct().Where(IsPotentialReference);
		}

		private bool IsPotentialReference(string value)
		{
			if (string.IsNullOrWhiteSpace(value))
			{
				return false;
			}

			var pattern = "\\w" + Seperator + "\\w";
			return Regex.IsMatch(value, pattern);
		}

		private void UpdateRelated()
		{
			if (string.IsNullOrEmpty(Key) || string.IsNullOrEmpty(Seperator))
				return;

			var childrenTask = DatabaseCommands.StartsWithAsync(Key + Seperator, 0, 15)
				.ContinueOnSuccess(items => items == null ? new string[0] : items.Select(i => i.Key));

			// find parent Ids
			var parentids = new List<string>();
			var id = Key;
			var lastindex = id.LastIndexOf(Seperator, StringComparison.Ordinal);

			while (!string.IsNullOrWhiteSpace(id) && lastindex != -1)
			{
				id = id.Remove(lastindex);
				parentids.Add(id);
				lastindex = id.LastIndexOf(Seperator, StringComparison.Ordinal);
			}

			var parentsTask = ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetAsync(parentids.ToArray(), null, metadataOnly: true)
				.ContinueOnSuccess(results => results.Results.Where(r => r != null).Select(r => r["@metadata"].SelectToken("@id").ToString()));


			TaskEx.WhenAll(childrenTask, parentsTask).ContinueOnSuccessInTheUIThread(t =>
			{
				var linkModels =
					childrenTask.Result
					.Concat(parentsTask.Result)
						.OrderBy(key => key.Length)
						.Select(key => new LinkModel
						{
							Title = key,
							HRef = "/Edit?id=" + key
						})
						.ToArray();

				Related.Set(linkModels);
			});
		}

		private void HandleDeleteDocument()
		{
			if (string.IsNullOrEmpty(DocumentKey))
			{
				return;
			}

			AskUser.ConfirmationAsync("Confirm Delete", string.Format("Are you sure you want do delete {0} ?", DocumentKey))
				.ContinueWhenTrueInTheUIThread(() => DoDeleteDocument(DocumentKey));
		}

		private void DoDeleteDocument(string documentKey)
		{
			DatabaseCommands.DeleteDocumentAsync(documentKey)
				.ContinueOnSuccessInTheUIThread(() =>
				{
					ApplicationModel.Current.AddNotification(new Notification(string.Format("Document {0} was deleted", documentKey)));
					if (CanNavigate && HasNext)
					{
						// navigate to the current index because the document has just been deleted, so another will move up to take its place
						var url = Navigator.GetUrlForCurrentIndex();
						if (url == UrlUtil.Url)
						{
							LoadModelParameters(string.Empty);
						}
						else
						{
							UrlUtil.Navigate(url);
						}
					}
					else if (CanNavigate)
					{
						UrlUtil.Navigate(Navigator.GetUrlForPrevious());
					}
					else
					{
						UrlUtil.Navigate(ParentPathSegments.Last().Url);
					}
				})
				.Catch();
		}

		protected override void OnViewLoaded()
		{
			ParserDispatcherManager.EnsureParserDispatcherIsCreated();

			DocumentOutliningMode mode = null;

			if (!string.IsNullOrEmpty(Settings.Instance.DocumentOutliningMode))
			{
				mode = OutliningModes.FirstOrDefault(m => m.Name == Settings.Instance.DocumentOutliningMode);
			}

			if (mode == null)
			{
				mode = OutliningModes.FirstOrDefault(m => m.Name == "Enabled");
			}

			SelectedOutliningMode = mode;

			if (Database.Value != null)
			{
				Database.Value.DocumentChanges.TakeUntil(Unloaded)
					.ObserveOnDispatcher()
					.Subscribe(n => HandleChangeNotification(n));
			}
		}

		private void HandleChangeNotification(DocumentChangeNotification notification)
		{
			if (notification.Name.Equals(DocumentKey, StringComparison.InvariantCulture))
			{
				if (notification.Type == DocumentChangeTypes.Put && notification.Etag != Etag)
				{
					ApplicationModel.Current.AddNotification(
							new Notification("Document " + Key + " was changed on the server"));
				}
				else if (notification.Type == DocumentChangeTypes.Delete)
				{
					ApplicationModel.Current.AddNotification(
							new Notification("Document " + Key + " was deleted on the server"));
				}
			}
		}

		public string Key
		{
			get { return document.Value.Key; }
			set
			{
				document.Value.Key = value;
				OnPropertyChanged(() => Key);
			}
		}

		public string Seperator
		{
			get
			{
				if (document.Value.Key != null && document.Value.Key.Contains("/"))
					return "/";
				if (document.Value.Key != null && document.Value.Key.Contains("-"))
					return "-";
				return null;
			}
		}

		public Guid? Etag
		{
			get { return document.Value.Etag; }
			set
			{
				document.Value.Etag = value;
				OnPropertyChanged(() => Etag);
				OnPropertyChanged(() => Metadata);
			}
		}

		public DateTime? LastModified
		{
			get { return document.Value.LastModified; }
			set
			{
				document.Value.LastModified = value;
				OnPropertyChanged(() => LastModified);
				OnPropertyChanged(() => Metadata);
			}
		}

		private IDictionary<string, string> metadata;
		private ICommand toggleExpansion;
		private DocumentOutliningMode outliningMode;
		private DocumentReferencedIdManager documentIdManager;
		private bool resolvingConflict;
		public static IList<DocumentOutliningMode> OutliningModes { get; private set; }


		public IEnumerable<KeyValuePair<string, string>> Metadata
		{
			get
			{
				return metadata
					.Where(x => x.Key != "@etag" && x.Key != "@id")
					.OrderBy(x => x.Key)
					.Concat(new[]
								{
									new KeyValuePair<string, string>("ETag", Etag.HasValue ? Etag.ToString() : ""),
									new KeyValuePair<string, string>("Last-Modified", LastModified.HasValue ? LastModified.ToString(): ""),
								});
			}
		}

		private bool IsDocumentValid()
		{
			return !EditorDocumentHasErrors(JsonDataDocument) && !EditorDocumentHasErrors(MetaDataDocument);
		}

		private bool EditorDocumentHasErrors(IEditorDocument editorDocument)
		{
			var parseData = editorDocument.ParseData as ILLParseData;
			return parseData != null && parseData.Errors.Any();
		}

		private Task WhenParsingComplete(IEditorDocument document)
		{
			var tcs = new TaskCompletionSource<bool>();
			if ((document.ParseData as ILLParseData).Snapshot == document.CurrentSnapshot)
			{
				tcs.SetResult(true);
			}
			else
			{
				EventHandler<ParseDataPropertyChangedEventArgs> completed = null;
				completed = (s, e) =>
								{
									tcs.SetResult(true);
									document.ParseDataChanged -= completed;
								};
				document.ParseDataChanged += completed;
			}

			return tcs.Task;
		}

		public override bool CanLeavePage()
		{
			if (HasUnsavedChanges)
			{
				return AskUser.Confirmation("Edit Document",
											"There are unsaved changes to this document. Are you sure you want to continue?");
			}
			else
			{
				return true;
			}
		}

		public ICommand Save
		{
			get { return new SaveDocumentCommand(this); }
		}

		public ICommand Delete
		{
			get { return deleteCommand ?? (deleteCommand = new ActionCommand(HandleDeleteDocument)); }
		}

		public ICommand Prettify
		{
			get { return new PrettifyDocumentCommand(this); }
		}

		public ICommand Refresh
		{
			get { return new RefreshDocumentCommand(this); }
		}

		public ICommand EnableSearch
		{
			get { return new ChangeFieldValueCommand<EditableDocumentModel>(this, x => x.SearchEnabled = true); }
		}

		public ICommand DisableSearch
		{
			get { return new ChangeFieldValueCommand<EditableDocumentModel>(this, x => x.SearchEnabled = false); }
		}

		public ICommand ToggleSearch
		{
			get { return new ChangeFieldValueCommand<EditableDocumentModel>(this, x => x.SearchEnabled = !x.searchEnabled); }
		}

		protected bool HasUnsavedChanges
		{
			get { return hasUnsavedChanges; }
			set { hasUnsavedChanges = value; }
		}

		private class RefreshDocumentCommand : Command
		{
			private readonly EditableDocumentModel parent;

			public RefreshDocumentCommand(EditableDocumentModel parent)
			{
				this.parent = parent;
			}

			public override bool CanExecute(object parameter)
			{
				return string.IsNullOrWhiteSpace(parent.DocumentKey) == false;
			}

			public override void Execute(object _)
			{
				if (parent.HasUnsavedChanges)
				{
					AskUser.ConfirmationAsync("Edit Document",
											  "There are unsaved changes to this document. Are you sure you want to refresh?")
						.ContinueWhenTrueInTheUIThread(DoRefresh);
				}
				else
				{
					DoRefresh();
				}
			}

			private void DoRefresh()
			{
				parent.DatabaseCommands.GetAsync(parent.DocumentKey)
					.ContinueOnSuccess(doc =>
										   {
											   if (doc == null)
											   {
												   parent.HandleDocumentNotFound();
												   return;
											   }

											   parent.document.Value = doc;
										   })
					.Catch();
			}
		}

		private class SaveDocumentCommand : Command
		{
			private readonly EditableDocumentModel parentModel;

			public string Seperator
			{
				get
				{
					if (parentModel.Key.Contains("/"))
						return "/";
					if (parentModel.Key.Contains("-"))
						return "-";
					return null;
				}
			}

			public SaveDocumentCommand(EditableDocumentModel parentModel)
			{
				this.parentModel = parentModel;
			}

			public override void Execute(object parameter)
			{
				TaskEx.WhenAll(parentModel.DocumentSections.Select(s => parentModel.WhenParsingComplete(s.Document)))
					.ContinueOnUIThread(t => ConfirmSave());
			}

			private void ConfirmSave()
			{
				if (!parentModel.IsDocumentValid())
				{
					this.parentModel.IsShowingErrors = true;
				}

				if (parentModel.Key != null && parentModel.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				{
					AskUser.ConfirmationAsync("Confirm Edit", "Are you sure that you want to edit a system document?")
						.ContinueWhenTrueInTheUIThread(SaveDocument);
					return;
				}

				if(parentModel.EditingDatabase)
				{
					AskUser.ConfirmationAsync("Confirm Edit", "Are you sure that you want to change the database document? This could cause irreversible damage")
						.ContinueWhenTrueInTheUIThread(SaveDatabaseDocument);
					return;
				}

				SaveDocument();
			}

			private void SaveDatabaseDocument()
			{
				RavenJObject doc;
				try
				{
					doc = RavenJObject.Parse(parentModel.JsonData);
				}
				catch (Exception ex)
				{
					ApplicationModel.Current.AddErrorNotification(ex, "Could not parse JSON");
					return;
				}


				var req = ApplicationModel.Current.Server.Value.DocumentStore
					.AsyncDatabaseCommands
					.ForDefaultDatabase()
					.CreateRequest("/admin/databases/" + ApplicationModel.Database.Value.Name, "PUT");

				req
					.WriteAsync(doc.ToString())
					.ContinueOnSuccess(() => req.ExecuteRequestAsync())
					.ContinueOnSuccessInTheUIThread(() =>
					{
						parentModel.HasUnsavedChanges = false;
						UrlUtil.Navigate("/settings");
					});
			}

			private void SaveDocument()
			{
				RavenJObject doc;
				RavenJObject metadata;

				try
				{
					doc = RavenJObject.Parse(parentModel.JsonData);
					metadata = RavenJObject.Parse(parentModel.JsonMetadata);
					if (parentModel.Key != null && Seperator != null && metadata.Value<string>(Constants.RavenEntityName) == null)
					{
						var entityName = parentModel.Key.Split(new[] { Seperator }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();

						if (entityName != null && entityName.Length > 1)
						{
							metadata[Constants.RavenEntityName] = char.ToUpper(entityName[0]) + entityName.Substring(1);
						}
						else
						{
							metadata[Constants.RavenEntityName] = entityName;
						}
					}
				}
				catch (Exception ex)
				{
					ApplicationModel.Current.AddErrorNotification(ex, "Could not parse JSON");
					return;
				}

				parentModel.UpdateMetadata(metadata);
				ApplicationModel.Current.AddInfoNotification("Saving document " + parentModel.Key + " ...");

				Guid? etag = string.Equals(parentModel.DocumentKey, parentModel.Key, StringComparison.InvariantCultureIgnoreCase) || parentModel.ResolvingConflict ?
					parentModel.Etag : Guid.Empty;

				DatabaseCommands.PutAsync(parentModel.Key, etag, doc, metadata)
					.ContinueOnSuccess(result =>
					{
						parentModel.ResolvingConflict = false;

						ApplicationModel.Current.AddInfoNotification("Document " + result.Key + " saved");
						parentModel.HasUnsavedChanges = false;
						parentModel.Etag = result.ETag;
						parentModel.PutDocumentKeyInUrl(result.Key, dontOpenNewTab: true);
						parentModel.SetCurrentDocumentKey(result.Key);
					})
					.ContinueOnSuccess(() => new RefreshDocumentCommand(parentModel).Execute(null))
					.Catch(exception => ApplicationModel.Current.AddErrorNotification(exception, "Could not save document."));
			}
		}



		private class PrettifyDocumentCommand : Command
		{
			private readonly EditableDocumentModel editableDocumentModel;

			public PrettifyDocumentCommand(EditableDocumentModel editableDocumentModel)
			{
				this.editableDocumentModel = editableDocumentModel;
			}

			public override void Execute(object parameter)
			{
				var document = editableDocumentModel.CurrentSection.Document;
				var formatter = document.Language.GetService<ITextFormatter>();
				if (formatter != null)
				{
					formatter.Format(document.CurrentSnapshot.SnapshotRange);
				}
			}
		}
	}

	public enum DocumentMode
	{
		NotInitializedYet,
		DocumentWithId,
		Projection,
		New,
	}

	public class DocumentSection
	{
		public string Name { get; set; }

		public IEditorDocument Document { get; set; }
	}

	public class DocumentError
	{
		public DocumentSection Section { get; set; }

		public IParseError ParseError { get; set; }
	}

	public class DocumentOutliningMode
	{
		public DocumentOutliningMode(string name)
		{
			Name = name;
		}

		public string Name { get; private set; }
		public Action<IEditorDocument> Applicator { get; set; }
	}
}
