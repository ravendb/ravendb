using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Input;
using Newtonsoft.Json;
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
			References = new ObservableCollection<string>();
			Related = new BindableCollection<string>();
			JsonData = newdoc.DataAsJson.ToString(Formatting.Indented);
			JsonMetadata = newdoc.Metadata.ToString(Formatting.Indented);
			Metadata = newdoc.Metadata.ToDictionary(x => x.Key, x => x.Value.ToString(Formatting.None));
			OnEverythingChanged();
		}

		public ObservableCollection<string> References { get; private set; }
		public BindableCollection<string> Related { get; private set; }
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
				References.Add(source);
			}
		}

		private void UpdateRelated()
		{
			asyncDatabaseCommands.GetDocumentsStartingWithAsync(Key + "/", 0, 15)
				.ContinueOnSuccess(items =>
				                   {
				                   	if (items == null)
				                   		return;

				                   	new Action(() => Related.Set(items.Select(doc => doc.Key))).ViaCurrentDispatcher();
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
			set { document.Etag = value; OnPropertyChanged(); }
		}

		public DateTime? LastModified
		{
			get { return document.LastModified; }
			set { document.LastModified = value; OnPropertyChanged(); }
		}

		public IDictionary<string, string> Metadata { get; private set; }

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
				try
				{
					editableDocumentModel.JsonData = RavenJObject.Parse(editableDocumentModel.JsonData).ToString(Formatting.Indented);
					editableDocumentModel.JsonMetadata = RavenJObject.Parse(editableDocumentModel.JsonMetadata).ToString(Formatting.Indented);
				}
				catch (JsonReaderException ex)
				{
					new ErrorWindow(ex.Message, string.Empty).Show();
				}
			}
		}
	}
}