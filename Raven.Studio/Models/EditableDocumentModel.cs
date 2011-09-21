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

namespace Raven.Studio.Models
{
	public class EditableDocumentModel : Model
	{
		private readonly JsonDocument document;
		public Observable<string> Notice { get; set; }
		private string jsonData;

		public EditableDocumentModel(JsonDocument document, IAsyncDatabaseCommands asyncDatabaseCommands)
		{
			this.document = document;
			this.asyncDatabaseCommands = asyncDatabaseCommands;
			IsProjection = string.IsNullOrEmpty(document.Key);
			References = new ObservableCollection<string>();
			Notice = new Observable<string>();
			JsonData = document.DataAsJson.ToString(Formatting.Indented);
			JsonMetadata = document.Metadata.ToString(Formatting.Indented);
			Metadata = document.Metadata.ToDictionary(x => x.Key, x => x.Value.ToString(Formatting.None));
		}

		public ObservableCollection<string> References { get; set; }
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
		private string notify;
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
						Notice.Value = "Document " + document.Key + " was deleted on the server";
					}
					else if (docOnServer.Etag != Etag)
					{
						Notice.Value = "Document " + document.Key + " was changed on the server";
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
			get { return new DeleteDocumentCommand(this.Key, asyncDatabaseCommands); }
		}

		public ICommand Prettify
		{
			get { return new PrettifyDocumentCommand(this); }
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
				var doc = RavenJObject.Parse(document.JsonData);
				var metadata = RavenJObject.Parse(document.JsonMetadata);

				document.Notice.Value = "saving document...";
				databaseCommands.PutAsync(document.Key, document.Etag,
										  doc,
										  metadata)
					.ContinueOnSuccess(result =>
					{
						document.Notice.Value = result.Key + " document saved";
						document.Etag = result.ETag;
					})
					.Catch(exception => document.Notice.Value = null);
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
				editableDocumentModel.JsonData = RavenJObject.Parse(editableDocumentModel.JsonData).ToString(Formatting.Indented);
				editableDocumentModel.JsonMetadata = RavenJObject.Parse(editableDocumentModel.JsonMetadata).ToString(Formatting.Indented);
			}
		}
	}
}