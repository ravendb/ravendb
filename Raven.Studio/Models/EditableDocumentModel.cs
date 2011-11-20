using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Input;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Features.Documents;
using Raven.Studio.Features.Input;
using Raven.Studio.Infrastructure;
using Raven.Studio.Messages;

namespace Raven.Studio.Models
{
	public class EditableDocumentModel : ViewModel
	{
		private readonly Observable<JsonDocument> document;
		private string jsonData;
		private bool isLoaded;
		private string documentKey;

		public EditableDocumentModel()
		{
			ModelUrl = "/edit";
			
			References = new ObservableCollection<LinkModel>();
			Related = new BindableCollection<LinkModel>(model => model.Title);

			document = new Observable<JsonDocument>();
			document.PropertyChanged += (sender, args) => UpdateFromDocument();
			document.Value = new JsonDocument { DataAsJson = new RavenJObject(), Metadata = new RavenJObject() };
		}

		public override void LoadModelParameters(string parameters)
		{
			var url = new UrlParser(UrlUtil.Url);

			if (url.GetQueryParam("mode") == "new")
			{
				Mode = DocumentMode.New;
				return;
			}

			var docId = url.GetQueryParam("id");
			if (string.IsNullOrWhiteSpace(docId) == false)
			{
				Mode = DocumentMode.DocumentWithId;
				documentKey = Key = docId;
				DatabaseCommands.GetAsync(docId)
					.ContinueOnSuccess(newdoc =>
					                   {
					                   	if (newdoc == null)
					                   	{
					                   		UrlUtil.Navigate("/DocumentNotFound?id=" + docId);
					                   		return;
					                   	}
					                   	document.Value = newdoc;
					                   	isLoaded = true;
					                   })
					.Catch();
				return;
			}

			var projection = url.GetQueryParam("projection");
			if (string.IsNullOrWhiteSpace(projection) == false)
			{
				Mode = DocumentMode.Projection;
				try
				{
					// TODO: this throwing an exception. Please load the projection form the query-string parameter.
					var newdoc = JsonConvert.DeserializeObject<JsonDocument>(Uri.UnescapeDataString(projection));
					document.Value = newdoc;
				}
				catch
				{
					UrlUtil.Navigate("/NotFound");
				}
			}
		}

		private void UpdateFromDocument()
		{
			var newdoc = document.Value;
			JsonMetadata = newdoc.Metadata.ToString(Formatting.Indented);
			UpdateMetadata(newdoc.Metadata);
			JsonData = newdoc.DataAsJson.ToString(Formatting.Indented);
			OnEverythingChanged();
		}

		private void UpdateMetadata(RavenJObject metadataAsJson)
		{
			metadata = metadataAsJson.ToDictionary(x => x.Key, x =>
															   {
																if (x.Value.Type == JTokenType.String)
																	return x.Value.Value<string>();
																return x.Value.ToString(Formatting.None);
															   });
			OnPropertyChanged("Metadata");
		}

		public ObservableCollection<LinkModel> References { get; private set; }
		public BindableCollection<LinkModel> Related { get; private set; }

		public string DisplayId
		{
			get
			{
				if (Mode == DocumentMode.Projection)
					return "Projection";
				if (Mode == DocumentMode.New)
					return "New Document";
				return Key;
			}
		}

		private string jsonMetadata;
		public string JsonMetadata
		{
			get { return jsonMetadata; }
			set
			{
				jsonMetadata = value;
				OnPropertyChanged();
				OnPropertyChanged("DocumentSize");
			}
		}

		private DocumentMode mode = DocumentMode.NotInitializedYet;
		public DocumentMode Mode
		{
			get { return mode; }
			set
			{
				mode = value;
				OnPropertyChanged();
				OnPropertyChanged("DisplayId");
			}
		}

		public string JsonData
		{
			get { return jsonData; }
			set
			{
				jsonData = value;
				UpdateReferences();
				UpdateRelated();
				OnPropertyChanged();
				OnPropertyChanged("DocumentSize");
			}
		}

		public string DocumentSize
		{
			get
			{
				double byteCount = Encoding.UTF8.GetByteCount(JsonData) + Encoding.UTF8.GetByteCount(JsonMetadata);
				string sizeTerm = "Bytes";
				if(byteCount > 1024*1024)
				{
					sizeTerm = "MBytes";
					byteCount = byteCount/1024*1024;
				}
				else if(byteCount > 1024)
				{
					sizeTerm = "KBytes";
					byteCount = byteCount / 1024;
			
				}
				return string.Format("Content-Length: {0:#,#.##} {1}", byteCount,sizeTerm);
			}
		}

		protected override Task LoadedTimerTickedAsync()
		{
			if (isLoaded && Mode != DocumentMode.DocumentWithId)
				return null;

			return DatabaseCommands.GetAsync(documentKey)
				.ContinueOnSuccess(docOnServer =>
				{
					if (docOnServer == null)
					{
						ApplicationModel.Current.AddNotification(new Notification("Document " + Key + " was deleted on the server"));
					}
					else if (docOnServer.Etag != Etag)
					{
						ApplicationModel.Current.AddNotification(new Notification("Document " + Key + " was changed on the server"));
					}
				});
		}

		private void UpdateReferences()
		{
			var referencesIds = Regex.Matches(jsonData, @"""(\w+/\w+)""");
			References.Clear();
			foreach (var source in referencesIds.Cast<Match>().Select(x => x.Groups[1].Value).Distinct())
			{
				References.Add(new LinkModel
							   {
								   Title = source,
								   HRef = "/Edit?id="+source
							   });
			}
		}

		private void UpdateRelated()
		{
			DatabaseCommands.GetDocumentsStartingWithAsync(Key + "/", 0, 15)
				.ContinueOnSuccess(items =>
								   {
									if (items == null)
										return;

									var linkModels = items.Select(doc => new LinkModel
																		 {
																			Title = doc.Key,
																			HRef = "/Edit?id=" + doc.Key
																		 }).ToArray();
									Related.Set(linkModels);
								   });
		}

		public string Key
		{
			get { return document.Value.Key; }
			set { document.Value.Key = value; OnPropertyChanged(); }
		}

		public Guid? Etag
		{
			get { return document.Value.Etag; }
			set
			{
				document.Value.Etag = value; 
				OnPropertyChanged();
				OnPropertyChanged("Metadata");
			}
		}

		public DateTime? LastModified
		{
			get { return document.Value.LastModified; }
			set {
				document.Value.LastModified = value;
				OnPropertyChanged();
				OnPropertyChanged("Metadata");
			}
		}

		private IDictionary<string, string> metadata;
		public IEnumerable<KeyValuePair<string, string>> Metadata
		{
			get
			{
				return metadata.OrderBy(x => x.Key)
					.Concat(new[]
								{
									new KeyValuePair<string, string>("ETag", Etag.HasValue ? Etag.ToString() : ""),
									new KeyValuePair<string, string>("Last-Modified", LastModified.HasValue ? LastModified.ToString(): ""),
								});
			}
		}

		public ICommand Save
		{
			get { return new SaveDocumentCommand(this, DatabaseCommands); }
		}

		public ICommand Delete
		{
			get { return new DeleteDocumentCommand(Key, DatabaseCommands, navigateToHome: true); }
		}

		public ICommand Prettify
		{
			get { return new PrettifyDocumentCommand(this); }
		}

		public ICommand Refresh
		{
			get { return new RefreshDocumentCommand(this); }
		}

		private class RefreshDocumentCommand : Command
		{
			private readonly EditableDocumentModel parent;

			public RefreshDocumentCommand(EditableDocumentModel parent)
			{
				this.parent = parent;
			}

			public override void Execute(object parameter)
			{
				parent.DatabaseCommands.GetAsync(parent.Key)
					.ContinueOnSuccess(doc =>
									   {
										   if (doc == null)
										   {
											   UrlUtil.Navigate("/DocumentNotFound?id=" + parent.Key);
											   return;
										   }

										   parent.document.Value = doc;
										   ApplicationModel.Current.AddNotification(new Notification(string.Format("Document {0} was refreshed", doc.Key)));
									   })
									   .Catch();
			}
		}

		private class SaveDocumentCommand : Command
		{
			private readonly EditableDocumentModel document;
			private readonly IAsyncDatabaseCommands databaseCommands;

			public SaveDocumentCommand(EditableDocumentModel document, IAsyncDatabaseCommands asyncDatabaseCommands)
			{
				this.document = document;
				this.databaseCommands = asyncDatabaseCommands;
			}

			public override void Execute(object parameter)
			{
				if (document.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				{
					AskUser.ConfirmationAsync("Confirm Edit", "Are you sure that you want to edit a system document?")
						.ContinueWhenTrue(SaveDocument);
					return;
				}

				SaveDocument();
			}

			private void SaveDocument()
			{
				RavenJObject doc;
				RavenJObject metadata;

				try
				{
					doc = RavenJObject.Parse(document.JsonData);
					metadata = RavenJObject.Parse(document.JsonMetadata);
				}
				catch (JsonReaderException ex)
				{
					ErrorPresenter.Show(ex.Message, string.Empty);
					return;
				}
				
				document.UpdateMetadata(metadata);
				ApplicationModel.Current.AddNotification(new Notification("Saving document " + document.Key + " ..."));
				databaseCommands.PutAsync(document.Key, document.Etag,
										  doc,
										  metadata)
					.ContinueOnSuccess(result =>
					{
						ApplicationModel.Current.AddNotification(new Notification("Document " + result.Key + " saved"));
						document.Etag = result.ETag;
					})
					.Catch(exception => ApplicationModel.Current.AddNotification(new Notification(exception.Message)));
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
				RavenJObject metadata;
				try
				{
					metadata = RavenJObject.Parse(editableDocumentModel.JsonMetadata);
					editableDocumentModel.JsonData = RavenJObject.Parse(editableDocumentModel.JsonData).ToString(Formatting.Indented);
					editableDocumentModel.JsonMetadata = metadata.ToString(Formatting.Indented);
				}
				catch (JsonReaderException ex)
				{
					ErrorPresenter.Show(ex.Message, string.Empty);
					return;
				}
				editableDocumentModel.UpdateMetadata(metadata);
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
}