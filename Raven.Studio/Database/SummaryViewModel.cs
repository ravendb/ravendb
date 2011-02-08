namespace Raven.Studio.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Globalization;
	using System.Linq;
	using System.Text;
	using System.Threading.Tasks;
	using System.Windows;
	using System.Windows.Data;
	using System.Windows.Markup;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Framework;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Plugin;
	using Raven.Database;

	public class SummaryViewModel : Screen, IRavenScreen
	{
		readonly IServer server;
		readonly DocumentTemplateProvider templateProvider;

		public SummaryViewModel(IServer server)
		{
			this.server = server;
			this.templateProvider = new DocumentTemplateProvider(server);
		}

		public string DatabaseName {get {return server.CurrentDatabase;}}

		public IEnumerable<DocumentViewModel> RecentDocuments { get; private set; }
		public IEnumerable<Collection> Collections { get; private set; }

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		protected override void OnActivate()
		{
			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetCollectionsAsync(0, 25)
					.ContinueOnSuccess(x =>
					{
						Collections = x.Result;
						NotifyOfPropertyChange(() => Collections);
					});

				session.Advanced.AsyncDatabaseCommands
					.GetDocumentsAsync(0, 10)
					.ContinueOnSuccess(x =>
					{
						RecentDocuments = x.Result.Select(doc => new DocumentViewModel(doc, templateProvider)).ToArray();
						NotifyOfPropertyChange(() => RecentDocuments);
					});
			}
		}

	}

	public static class DictionaryExtensions
	{
		public static T IfPresent<T>(this IDictionary<string, JToken> dictionary, string key)
		{
			return dictionary.ContainsKey(key) ? dictionary[key].Value<T>() : default(T);
		}
	}

	public class DocumentViewModel : PropertyChangedBase
	{
		public const int SummaryLength = 150;

		readonly IDictionary<string, JToken> data;
		readonly IDictionary<string, JToken> metadata;

		public DocumentViewModel(JsonDocument document, DocumentTemplateProvider templateProvider)
		{
			data = new Dictionary<string, JToken>();
			metadata = new Dictionary<string, JToken>();

			JsonData = PrepareRawJsonString(document.DataAsJson);
			//JsonMetadata = PrepareRawJsonString(document.Metadata);

			Id = document.Key;
			//data = ParseJsonToDictionary(document.DataAsJson);
			metadata = ParseJsonToDictionary(document.Metadata);

			LastModified = metadata.IfPresent<DateTime>("Last-Modified");
			CollectionType = metadata.IfPresent<string>("Raven-Entity-Name");
			ClrType = metadata.IfPresent<string>("Raven-Clr-Type");
			var templateKey = metadata.IfPresent<string>("Raven-View-Template");

			templateProvider
				.GetTemplateFor(templateKey)
				.ContinueOnSuccess(x=>
					{
						DataTemplate = x.Result;
						NotifyOfPropertyChange(() => DataTemplate);
					});
		}

		public DataTemplate DataTemplate {get;private set;}
		public string ClrType {get; private set;}
		public string CollectionType {get; private set;}
		public DateTime LastModified {get; private set;}

		public string Id { get; private set; }

		public string JsonData { get; private set; }

		public string JsonMetadata { get; private set; }

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

		public IDictionary<string, JToken> Data
		{
			get { return data; }
		}

		public IDictionary<string, JToken> Metadata
		{
			get { return metadata; }
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

	public class DocumentTemplateProvider
	{
		readonly IServer server;
		readonly Dictionary<string,DataTemplate> templates = new Dictionary<string, DataTemplate>();

		public DocumentTemplateProvider(IServer server)
		{
			this.server = server;
		}

		static string GetDefaultTemplateXaml()
		{
			return @"
                <Border Height='150' Width='110' BorderBrush='LightGray' BorderThickness='1'>
                    <TextBlock Text='{Binding JsonData}' TextWrapping='Wrap' />
                </Border>";
		}

		public Task<DataTemplate> GetTemplateFor(string key)
		{
			var tcs = new TaskCompletionSource<DataTemplate>();

			if(string.IsNullOrEmpty(key))
			{
				var defaultTemplate = Create(GetDefaultTemplateXaml());
				tcs.TrySetResult(defaultTemplate);
			}
			else if (templates.ContainsKey(key))
			{
				tcs.TrySetResult(templates[key]);
			} else
			{
				using(var session = server.OpenSession())
				{
					session.Advanced.AsyncDatabaseCommands
						.GetAttachmentAsync(key)
						.ContinueWith(task=>
						{
						    if(task.IsFaulted)
						    { 
								throw new NotImplementedException("What to do in this case?");
						    } else
						    {
								var xaml = task.Result.Data.ToString();
								var template = Create(xaml);
								templates[key] = template;
								tcs.TrySetResult(template);
						    }
						});
				}
			}

			return tcs.Task;
		}


		public static DataTemplate Create(string innerXaml)
		{
			DataTemplate template = null;
		Execute.OnUIThread(()=> { template = 
			 (DataTemplate)XamlReader.Load(
				@"<DataTemplate xmlns=""http://schemas.microsoft.com/client/2007"">" + innerXaml + @"</DataTemplate>"
			  );
			  }
		);
		return template;
		}
	}
}