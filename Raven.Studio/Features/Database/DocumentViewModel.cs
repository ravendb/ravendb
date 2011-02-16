namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using System.Windows;
	using Caliburn.Micro;
	using Framework;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Raven.Database;
	using Shell;

	[Export]
	[PartCreationPolicy(CreationPolicy.NonShared)]
	public class DocumentViewModel : Screen
	{
		public const int SummaryLength = 150;

		readonly IDictionary<string, JToken> data;
		readonly IEventAggregator events;
		readonly NavigationViewModel navigation;
		readonly DocumentTemplateProvider templateProvider;
		string id;
		string jsonData;
		JsonDocument jsonDocument;
		IDictionary<string, JToken> metadata;

		[ImportingConstructor]
		public DocumentViewModel(DocumentTemplateProvider templateProvider, NavigationViewModel navigation,
		                         IEventAggregator events)
		{
			this.templateProvider = templateProvider;
			this.navigation = navigation;
			this.events = events;

			data = new Dictionary<string, JToken>();
			metadata = new Dictionary<string, JToken>();

			Id = "";
			jsonDocument = new JsonDocument();
			JsonData = @"{
	""PropertyName"": """"
}";

			events.Subscribe(this);
		}

		public DataTemplate DataTemplate { get; private set; }
		public string ClrType { get; private set; }
		public string CollectionType { get; private set; }
		public DateTime LastModified { get; private set; }

		public string Id
		{
			get { return id; }
			set
			{
				id = value ?? string.Empty;
				NotifyOfPropertyChange(() => Id);
				NotifyOfPropertyChange(() => DisplayId);
			}
		}

		public string DisplayId
		{
			get
			{
				if( string.IsNullOrEmpty(id)) return "Auto Generate Id";

				var collectionType = CollectionType + "/";
				var display = Id
					.Replace(collectionType, string.Empty)
					.Replace(collectionType.ToLower(), string.Empty)
					.Replace("Raven/", string.Empty);

				Guid guid;
				if (Guid.TryParse(display, out guid))
				{
					display = display.Substring(0, 8);
				}
				return display;
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

		public string JsonMetadata { get; private set; }

		public string Summary
		{
			get
			{
				return (JsonData.Length > SummaryLength
				        	? JsonData.Substring(0, SummaryLength) + "..."
				        	: JsonData)
					.Replace("\r", "").Replace("\n", " ");
			}
		}

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

		public DocumentViewModel Initialize(JsonDocument document)
		{
			jsonDocument = document;
			JsonData = PrepareRawJsonString(document.DataAsJson);
			//JsonMetadata = PrepareRawJsonString(document.Metadata);

			Id = document.Key;
			//data = ParseJsonToDictionary(document.DataAsJson);
			metadata = ParseJsonToDictionary(document.Metadata);

			LastModified = metadata.IfPresent<DateTime>("Last-Modified");
			CollectionType = DetermineCollectionType();
			ClrType = metadata.IfPresent<string>("Raven-Clr-Type");

			templateProvider
				.GetTemplateFor(CollectionType ?? "default")
				.ContinueOnSuccess(x =>
				                   	{
				                   		DataTemplate = x.Result;
				                   		NotifyOfPropertyChange(() => DataTemplate);
				                   	});

			DisplayName = DisplayId;

			return this;
		}

		public void PrepareForSave()
		{
			//if (!ValidateJson(JsonData))
			//{
			//    WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document)", parseExceptionMessage));
			//    return;
			//}

			//if (!ValidateJson(JsonMetadata))
			//{
			//    WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document Metadata)", parseExceptionMessage));
			//    return;
			//}

			jsonDocument.DataAsJson = JObject.Parse(JsonData);
			//jsonDocument.Metadata = JObject.Parse(JsonMetadata);
			jsonDocument.Key = Id;
		}

		public void Prettify()
		{
			//NOTE: is there a better way to reformat the json? This seems heavy.
			var obj = JsonConvert.DeserializeObject(JsonData);
			JsonData = JsonConvert.SerializeObject(obj, Formatting.Indented);
		}

		//NOTE: quick hack to get me focused on more important things
		public DocumentViewModel CloneUsing(JsonDocument document)
		{
			var doc = new DocumentViewModel(templateProvider, navigation, events);
			return doc.Initialize(document);
		}

		string DetermineCollectionType()
		{
			return metadata.IfPresent<string>("Raven-Entity-Name") ?? (Id.StartsWith("Raven/") ? "System" : null) ?? "document";
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

		static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data)
		{
			var result = new StringBuilder("{\n");

			foreach (var item in data)
			{
				result.AppendFormat("\"{0}\" : {1},\n", item.Key, item.Value);
			}
			result.Append("}");

			return result.ToString();
		}
	}
}