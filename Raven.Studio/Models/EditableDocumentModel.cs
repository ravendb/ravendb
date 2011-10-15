using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
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
	public class EditableDocumentModel : Model
	{
		private JsonDocument document;
		private string jsonData;

		public EditableDocumentModel(JsonDocument document, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			UpdateFromDocument(document);
		}

		private void UpdateFromDocument(JsonDocument newdoc)
		{
			this.document = newdoc;
			IsProjection = string.IsNullOrEmpty(newdoc.Key);
			References = new ObservableCollection<LinkModel>();
			Related = new BindableCollection<LinkModel>(new PrimaryKeyComparer<LinkModel>(model => model.HRef));
			JsonData = newdoc.DataAsJson.ToString(Formatting.Indented);
			JsonMetadata = newdoc.Metadata.ToString(Formatting.Indented);
			UpdateMetadata(newdoc.Metadata);
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
		public bool IsProjection { get; private set; }

		public string DisplayId
		{
			get
			{
				if (IsProjection) return "Projection";
				return string.IsNullOrEmpty(Key)
					? "New Document"
					: Key;
			}
		}

		private string jsonMetadata;
		private IAsyncDatabaseCommands asyncDatabaseCommands;

		public string JsonMetadata
		{
			get { return jsonMetadata; }
			set { jsonMetadata = value; OnPropertyChanged(); }
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
			}
		}

		protected override Task TimerTickedAsync()
		{
			if (string.IsNullOrEmpty(document.Key))
				return null;

			return asyncDatabaseCommands.GetAsync(document.Key)
				.ContinueOnSuccess(docOnServer =>
				{
					if (docOnServer == null)
					{
						ApplicationModel.Current.AddNotification(new Notification("Document " + document.Key + " was deleted on the server"));
					}
					else if (docOnServer.Etag != Etag)
					{
						ApplicationModel.Current.AddNotification(new Notification("Document " + document.Key + " was changed on the server"));
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
			asyncDatabaseCommands.GetDocumentsStartingWithAsync(Key + "/", 0, 15)
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
			get { return document.Key; }
			set { document.Key = value; OnPropertyChanged(); }
		}

		public Guid? Etag
		{
			get { return document.Etag; }
			set
			{
			    document.Etag = value; 
                OnPropertyChanged();
                OnPropertyChanged("Metadata");
			}
		}

		public DateTime? LastModified
		{
			get { return document.LastModified; }
			set { 
                document.LastModified = value;
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
			get { return new SaveDocumentCommand(this, asyncDatabaseCommands); }
		}

		public ICommand Delete
		{
			get { return new DeleteDocumentCommand(this.Key, asyncDatabaseCommands, navigateToHome: true); }
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
				parent.asyncDatabaseCommands.GetAsync(parent.Key)
					.ContinueOnSuccess(doc =>
									   {
										   if (doc == null)
										   {
											   ApplicationModel.Current.Navigate(new Uri("/DocumentNotFound?id=" + parent.Key,
																						 UriKind.Relative));
											   return;
										   }

										   parent.UpdateFromDocument(doc);
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
					new ErrorWindow(ex.Message, string.Empty).Show();
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
					new ErrorWindow(ex.Message, string.Empty).Show();
					return;
				}
				editableDocumentModel.UpdateMetadata(metadata);
			}
		}
	}
}