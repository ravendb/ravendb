namespace Raven.Studio.Features.Documents
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using Caliburn.Micro;
	using Framework;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Raven.Database;
	using System.Linq;

    [Export(typeof(EditDocumentViewModel))]
	[PartCreationPolicy(CreationPolicy.NonShared)]
	public class EditDocumentViewModel : Screen
	{
		readonly IEventAggregator events;
		JsonDocument document;
		string id;
		string jsonData;
		string jsonMetadata;
		IDictionary<string, JToken> metadata;

		[ImportingConstructor]
		public EditDocumentViewModel(IEventAggregator events)
		{
			this.events = events;

			metadata = new Dictionary<string, JToken>();

			Id = "";
			document = new JsonDocument();
			JsonData = InitialJsonData();
			events.Subscribe(this);
		}

		public string ClrType { get; private set; }
		public string CollectionType { get; private set; }
		public DateTime LastModified { get; private set; }

		public string Id
		{
			get { return IsProjection ? "Projection" : id; }
			set
			{
				id = value ?? string.Empty;
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

		public IDictionary<string, JToken> Metadata
		{
			get { return metadata; }
		}

		public JsonDocument JsonDocument
		{
			get { return document; }
		}

		public bool IsProjection { get; private set; }

		public void Initialize(JsonDocument doc)
		{
		    document = doc;

		    UpdateDocumentFromJsonDocument();
		}

	    public void UpdateDocumentFromJsonDocument()
	    {
	        Id = document.Key;
	        JsonData = PrepareRawJsonString(document.DataAsJson);

	        IsProjection = string.IsNullOrEmpty(Id) && (document.Metadata == null);

	        if (IsProjection) return;

            if (document.Metadata != null)
            {
                foreach (var property in document.Metadata.Properties().ToList())
                {
                    if(property.Name.StartsWith("@"))
                        property.Remove();
                }
            }

	        JsonMetadata = PrepareRawJsonString(document.Metadata);

	        metadata = ParseJsonToDictionary(document.Metadata);

	        LastModified = metadata.IfPresent<DateTime>("Last-Modified");
	        CollectionType = DocumentViewModel.DetermineCollectionType(document.Metadata);
	        ClrType = metadata.IfPresent<string>("Raven-Clr-Type");
	    }

	    public void PrepareForSave()
		{
			document.DataAsJson = ToJObject(JsonData);
			document.Metadata = ToJObject(JsonMetadata);
			document.Key = Id;

			LastModified = DateTime.Now;
			metadata = ParseJsonToDictionary(document.Metadata);
			NotifyOfPropertyChange(() => Metadata);
		}

		static JObject ToJObject(string json)
		{
			return string.IsNullOrEmpty(json) ? new JObject() : JObject.Parse(json);
		}

		public void Prettify()
		{
			//NOTE: is there a better way to reformat the json? This seems heavy.
			JsonData = JsonConvert.SerializeObject(JsonConvert.DeserializeObject(JsonData), Formatting.Indented);
			JsonMetadata = JsonConvert.SerializeObject(JsonConvert.DeserializeObject(JsonMetadata), Formatting.Indented);
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

		static string InitialJsonData()
		{
			return @"{
	""PropertyName"": """"
}";
		}
	}
}