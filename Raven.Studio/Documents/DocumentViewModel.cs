namespace Raven.Studio.Documents
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using System.Windows;
	using System.Windows.Markup;
	using Caliburn.Micro;
	using Controls;
	using Database;
	using Dialogs;
	using Framework;
	using Messages;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;
	using Shell;

	public class DocumentViewModel1 : Screen, IRavenScreen
	{
		public const int SummaryLength = 150;

		static readonly HashSet<string> FilteredMetadataKeys = new HashSet<string>
		                                                       	{
		                                                       		"@id",
		                                                       		"@etag",
		                                                       		"Non-Authoritive-Information",
		                                                       		"Last-Modified"
		                                                       	};

		readonly IDictionary<string, JToken> data;
		readonly IServer server;
		readonly JsonDocument jsonDocument;
		readonly IDictionary<string, JToken> metadata;

		string id;
		bool isSelected;
		string jsonData;
		string jsonMetadata;

		public DocumentViewModel1(JsonDocument document, IServer server)
		{
			DisplayName = "Edit Document";

			Thumbnail = new DocumentThumbnail();
			this.server = server;
			CompositionInitializer.SatisfyImports(this);

			data = new Dictionary<string, JToken>();
			metadata = new Dictionary<string, JToken>();

			jsonData = PrepareRawJsonString(document.DataAsJson, false);
			jsonMetadata = PrepareRawJsonString(document.Metadata, true);
			CustomizedThumbnailTemplate = CreateThumbnailTemplate(document.Metadata);

			Id = document.Key;
			data = ParseJsonToDictionary(document.DataAsJson);
			metadata = ParseJsonToDictionary(document.Metadata);

			jsonDocument = document;
		}

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		[Import]
		public IWindowManager WindowManager { get; set; }

		public DocumentThumbnail Thumbnail { get; set; }

		public FrameworkElement CustomizedThumbnailTemplate { get; set; }

		public string Id
		{
			get { return id; }
			set
			{
				id = value;
				NotifyOfPropertyChange(() => Id);
			}
		}

		public string JsonData
		{
			get { return jsonData; }
			set
			{
				jsonData = value;
				NotifyOfPropertyChange(() => JsonData);
			}
		}

		public string JsonMetadata
		{
			get { return jsonMetadata; }
			set
			{
				jsonMetadata = value;
				NotifyOfPropertyChange(() => JsonMetadata);
			}
		}

		public string Summary
		{
			get
			{
				if (JsonData.Length > SummaryLength)
				{
					return JsonData.Substring(0, SummaryLength)
					       	.Replace("\r", "").Replace("\n", " ") + "...";
				}
				return JsonData.Replace("\r", "").Replace("\n", " ");
			}
		}

		public bool IsSelected
		{
			get { return isSelected; }

			set
			{
				isSelected = value;
				NotifyOfPropertyChange(() => IsSelected);
			}
		}

		#region not sure how these are used yet

		public IDictionary<string, JToken> Data
		{
			get { return data; }
		}

		public IDictionary<string, JToken> Metadata
		{
			get { return metadata; }
		}

		public JsonDocument JsonDocument
		{
			get { return jsonDocument; }
		}

		#endregion

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		static IDictionary<string, JToken> ParseJsonToDictionary(JObject dataAsJson)
		{
			IDictionary<string, JToken> result = new Dictionary<string, JToken>();

			foreach (var d in dataAsJson)
			{
				result.Add(d.Key, d.Value);
			}

			return result;
		}

		public void SelectUnselect()
		{
			IsSelected = !IsSelected;
		}

		public void Preview()
		{
			var vm = IoC.Get<DocumentViewModel>();
			var documentScreen = (BrowseDocumentsViewModel) Parent;
			documentScreen.ActivateItem(vm.CloneUsing(jsonDocument));
            //documentScreen.IsDocumentPreviewed = true;
		}

		string parseExceptionMessage;
		bool ValidateJson(string json)
		{
			try
			{
				JObject.Parse(json);
				parseExceptionMessage = string.Empty;
				return true;
			}
			catch (JsonReaderException exception)
			{
				parseExceptionMessage = exception.Message;
				return false;
			}
		}

		public void Save()
		{
			if (!ValidateJson(JsonData))
			{
				WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document)", parseExceptionMessage));
				return;
			}

			if (!ValidateJson(JsonMetadata))
			{
				WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document Metadata)", parseExceptionMessage));
				return;
			}

			jsonDocument.DataAsJson = JObject.Parse(JsonData);
			jsonDocument.Metadata = JObject.Parse(JsonMetadata);
			jsonDocument.Key = id;

			using (var session = server.OpenSession())
			session.Advanced.AsyncDatabaseCommands
				.PutAsync(jsonDocument.Key, null, jsonDocument.DataAsJson, jsonDocument.Metadata)
				.ContinueOnSuccess(task =>
				{
					Id = task.Result.Key;
				});
		}

		public void ShowDocument()
		{
			EventAggregator.Publish(new ReplaceActiveScreen(this));
		}

		static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data, bool withFilter)
		{
			var result = new StringBuilder("{\n");

			foreach (var item in data)
			{
				if (!withFilter || !FilteredMetadataKeys.Contains(item.Key))
				{
					result.AppendFormat("\"{0}\" : {1},\n", item.Key, item.Value);
				}
			}
			result.Append("}");

			return result.ToString();
		}

		static FrameworkElement CreateThumbnailTemplate(IDictionary<string, JToken> metadata)
		{
			FrameworkElement thumbnailTemplate;

			if (metadata.ContainsKey("Raven-View-Template"))
			{
				string data = metadata["Raven-View-Template"].ToString();

				string xaml = data.Substring(data.IndexOf('"') + 1, data.LastIndexOf('"') - 1);

				thumbnailTemplate = (FrameworkElement) XamlReader.Load(xaml);
			}
			else
			{
				thumbnailTemplate =
					(FrameworkElement)
					XamlReader.Load(
						@"
                <Border xmlns='http://schemas.microsoft.com/winfx/2006/xaml/presentation' 
                        Height='150' Width='110' BorderBrush='LightGray' BorderThickness='1'>
                    <TextBlock Text='{Binding JsonData}' TextWrapping='Wrap' />
                </Border>");
			}

			return thumbnailTemplate;
		}
	}
}