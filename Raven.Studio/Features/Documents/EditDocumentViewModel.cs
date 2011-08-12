using Raven.Json.Linq;
using Raven.Studio.Framework.Extensions;
using Raven.Studio.Infrastructure.Navigation;

namespace Raven.Studio.Features.Documents
{
	using System.Windows;
	using System.Windows.Input;
	using Commands;
	using System;
	using System.Collections.Generic;
	using System.Collections.ObjectModel;
	using System.ComponentModel.Composition;
	using System.Linq;
	using System.Text.RegularExpressions;
	using Caliburn.Micro;
	using Newtonsoft.Json;
	using Abstractions.Data;
	using Framework;

	[Export(typeof(EditDocumentViewModel))]
	[PartCreationPolicy(CreationPolicy.NonShared)]
	public class EditDocumentViewModel : RavenScreen
	{
		private JsonDocument document;
		private string etag;
		private string id;
		private string jsonData;
		private string jsonMetadata;
		private DateTime lastModified;
		private IDictionary<string, RavenJToken> metadata;
		private bool nonAuthoritiveInformation;
		private readonly BindableCollection<string> references = new BindableCollection<string>();
		private IKeyboardShortcutBinder keys;

		[ImportingConstructor]
		public EditDocumentViewModel(IKeyboardShortcutBinder keys)
		{
			metadata = new Dictionary<string, RavenJToken>();

			Id = "";
			document = new JsonDocument();
			JsonData = InitialJsonData();
			JsonMetadata = "{}";
			Events.Subscribe(this);
			this.keys = keys;

			keys.Register<SaveDocument>(Key.S, ModifierKeys.Control, x => x.Execute(this), this);
		}

		protected override void OnViewAttached(object view, object context)
		{
			keys.Initialize((FrameworkElement)view);
			base.OnViewAttached(view, context);
		}

		protected override void OnActivate()
		{
			base.OnActivate();
			Server.OpenSession().Advanced.AsyncDatabaseCommands
						.GetDocumentsStartingWithAsync(Id +"/", 0, 15)
						.ContinueOnSuccess(get =>
						{
							if (get.Result == null)
								return;

							Execute.OnUIThread(() =>
							{
								related.Clear();
								related.AddRange(get.Result.Select(doc => doc.Key));
							});
						});
		}

		protected override NavigationState GetScreenNavigationState()
		{
			return new NavigationState { Url = "docs/" + Id, Title = string.Format("Edit Document {0}", DisplayId) };
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

		public string DisplayId
		{
			get
			{
				if (IsProjection) return "Projection";
				return string.IsNullOrEmpty(Id)
					? "New Document"
					: Id;
			}
		}

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
			references.AddRange(referencesIds.Cast<Match>().Select(x=>x.Groups[1].Value).Distinct());
		}

		public ObservableCollection<string> References
		{
			get { return references; }
		}

		private readonly BindableCollection<string> related = new BindableCollection<string>();
		public BindableCollection<string> Related
		{
			get
			{
				return related;
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

		public IDictionary<string, RavenJToken> Metadata
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

			IsProjection = string.IsNullOrEmpty(Id) && (document.Metadata == null || document.Metadata.Any() == false);
			if (IsProjection) return;

			if (document.Metadata != null)
			{
				foreach (var property in document.Metadata.ToList())
				{
					if (property.Key.StartsWith("@"))
						document.Metadata.Remove(property.Key);
				}
			}

			JsonMetadata = PrepareRawJsonString(document.Metadata);

			metadata = ParseJsonToDictionary(document.Metadata);

			LastModified = document.LastModified ?? DateTime.MinValue;
			if (LastModified.Kind == DateTimeKind.Utc)
				LastModified = LastModified.ToLocalTime();
			CollectionType = DocumentViewModel.DetermineCollectionType(document.Metadata);
			ClrType = metadata.IfPresent<string>(Constants.RavenClrType);
			Etag = document.Etag.ToString();
			NonAuthoritiveInformation = document.NonAuthoritiveInformation ?? false;
		}

		public void PrepareForSave()
		{
			document.DataAsJson = ToJObject(JsonData);
			document.Metadata = ToJObject(JsonMetadata);
			document.Key = Id;

			// user create a document with key like:
			// users/1234 but didn't specify the Raven-Entity-Name, let 
			// us provide one for him
			if (document.Key != null &&
				document.Key.Contains('/') &&
				document.Metadata[Constants.RavenEntityName] == null)
			{
				var indexOf = document.Key.IndexOf('/');
				document.Metadata[Constants.RavenEntityName] = char.ToUpper(document.Key[0]) + document.Key.Substring(1, indexOf - 1);
			}

			LastModified = DateTime.Now;
			metadata = ParseJsonToDictionary(document.Metadata);
			NotifyOfPropertyChange(() => Metadata);
		}

		private static RavenJObject ToJObject(string json)
		{
			return string.IsNullOrEmpty(json) ? new RavenJObject() : RavenJObject.Parse(json);
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

		private static IDictionary<string, RavenJToken> ParseJsonToDictionary(RavenJObject dataAsJson)
		{
			return dataAsJson.ToDictionary(d => d.Key, d => d.Value);
		}

		private static string PrepareRawJsonString(RavenJObject json)
		{
			return json.ToString(Formatting.Indented);
		}

		private static string InitialJsonData()
		{
			return @"{
	""PropertyName"": """"
}";
		}
	}
}