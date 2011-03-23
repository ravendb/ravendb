using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel.Composition;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Caliburn.Micro;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Studio.Framework;

namespace Raven.Studio.Features.Documents
{
	[Export(typeof (EditDocumentViewModel))]
	[PartCreationPolicy(CreationPolicy.NonShared)]
	public class EditDocumentViewModel : Screen
	{
		private JsonDocument document;
		private string etag;
		private string id;
		private string jsonData;
		private string jsonMetadata;
		private DateTime lastModified;
		private IDictionary<string, JToken> metadata;
		private bool nonAuthoritiveInformation;
		private readonly ObservableCollection<string> references = new ObservableCollection<string>();

		[ImportingConstructor]
		public EditDocumentViewModel(IEventAggregator events)
		{
			metadata = new Dictionary<string, JToken>();

			Id = "";
			document = new JsonDocument();
			JsonData = InitialJsonData();
			events.Subscribe(this);
		}

		public string ClrType { get; private set; }
		public string CollectionType { get; private set; }

		public DateTime LastModified
		{
			get { return lastModified; }
			set
			{
				lastModified = value;
				NotifyOfPropertyChange(() => LastModified);
			}
		}

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
				UpdateReferences();
				NotifyOfPropertyChange(() => JsonData);
			}
		}

		private void UpdateReferences()
		{
			var referencesIds = Regex.Matches(jsonData, @"""(\w+/\w+)""");
			references.Clear();
			foreach (Match match in referencesIds)
			{
				references.Add(match.Groups[1].Value);
			}
		}

		public ObservableCollection<string> References
		{
			get { return references; }
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

		public bool NonAuthoritiveInformation
		{
			get { return nonAuthoritiveInformation; }
			set
			{
				nonAuthoritiveInformation = value;
				NotifyOfPropertyChange(() => NonAuthoritiveInformation);
			}
		}


		public string Etag
		{
			get { return etag; }
			set
			{
				etag = value;
				NotifyOfPropertyChange(() => Etag);
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
				foreach (JProperty property in document.Metadata.Properties().ToList())
				{
					if (property.Name.StartsWith("@"))
						property.Remove();
				}
			}

			JsonMetadata = PrepareRawJsonString(document.Metadata);

			metadata = ParseJsonToDictionary(document.Metadata);

			LastModified = document.LastModified;
			CollectionType = DocumentViewModel.DetermineCollectionType(document.Metadata);
			ClrType = metadata.IfPresent<string>("Raven-Clr-Type");
			Etag = document.Etag.ToString();
			NonAuthoritiveInformation = document.NonAuthoritiveInformation;
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

		private static JObject ToJObject(string json)
		{
			return string.IsNullOrEmpty(json) ? new JObject() : JObject.Parse(json);
		}

		public void Prettify()
		{
			JsonData = Prettify(JsonData);
			JsonMetadata = Prettify(JsonMetadata);
		}

		private static string Prettify(string json)
		{
			//NOTE: is there a better way to reformat the json? This seems heavy.
			return JsonConvert.SerializeObject(JsonConvert.DeserializeObject(json), Formatting.Indented);
		}

		private static IDictionary<string, JToken> ParseJsonToDictionary(JObject dataAsJson)
		{
			IDictionary<string, JToken> result = new Dictionary<string, JToken>();

			foreach (var d in dataAsJson)
			{
				result.Add(d.Key, d.Value);
			}

			return result;
		}

		private static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data)
		{
			StringBuilder result = new StringBuilder().AppendLine("{");

			foreach (var item in data)
			{
				result.AppendFormat("\t\"{0}\" : {1},", item.Key, item.Value)
					.AppendLine();
			}
			result.AppendLine("}");

			return result.ToString();
		}

		private static string InitialJsonData()
		{
			return @"{
	""PropertyName"": """"
}";
		}
	}
}