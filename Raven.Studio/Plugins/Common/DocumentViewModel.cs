namespace Raven.Studio.Plugins.Common
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Text;
	using System.Windows;
	using System.Windows.Markup;
	using Caliburn.Micro;
	using Controls;
	using Dialogs;
	using Documents;
	using Documents.Browse;
	using Messages;
	using Newtonsoft.Json.Linq;
	using Plugin;

	public class DocumentViewModel : Screen, IRavenScreen
	{
		public const int SummaryLength = 150;

		static readonly HashSet<string> FilteredMetadataKeys = new HashSet<string>
		                                                       	{
		                                                       		"@id",
		                                                       		"@etag",
		                                                       		"Non-Authoritive-Information",
		                                                       		"Last-Modified"
		                                                       	};

		readonly Document _document;
		string _id;
		bool _isSelected;
		string _jsonData;
		string _jsonMetadata;

		public DocumentViewModel(Document document, IDatabase database)
		{
			DisplayName = "Edit Document";
			_document = document;
			_id = document.Id;
			_jsonData = PrepareRawJsonString(document.Data, false);
			_jsonMetadata = PrepareRawJsonString(document.Metadata, true);
			Thumbnail = new DocumentThumbnail();
			Database = database;
			CompositionInitializer.SatisfyImports(this);
			CustomizedThumbnailTemplate = CreateThumbnailTemplate(document.Metadata);
		}

		public DocumentThumbnail Thumbnail { get; set; }

		public FrameworkElement CustomizedThumbnailTemplate { get; set; }

		public string Id
		{
			get { return _id; }
			set
			{
				_id = value;
				NotifyOfPropertyChange(() => Id);
			}
		}

		[Import]
		public IEventAggregator EventAggregator { get; set; }

		[Import]
		public IWindowManager WindowManager { get; set; }

		public IDatabase Database { get; set; }

		public string JsonData
		{
			get { return _jsonData; }
			set
			{
				_jsonData = value;
				NotifyOfPropertyChange(() => JsonData);
			}
		}

		public string JsonMetadata
		{
			get { return _jsonMetadata; }
			set
			{
				_jsonMetadata = value;
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
			get { return _isSelected; }

			set
			{
				_isSelected = value;
				NotifyOfPropertyChange(() => IsSelected);
			}
		}

		public SectionType Section
		{
			get { return SectionType.Documents; }
		}

		public void SelectUnselect()
		{
			IsSelected = !IsSelected;
		}

		public void Preview()
		{
			var documentScreen = (DocumentsScreenViewModel) Parent;
			documentScreen.ActivateItem(this);
			documentScreen.IsDocumentPreviewed = true;
		}

		public void Save()
		{
			if (Document.ValidateJson(JsonData))
			{
				if (Document.ValidateJson(JsonMetadata))
				{
					_document.SetData(JsonData);
					_document.SetMetadata(JsonMetadata);

					_document.SetId(Id);
					_document.Save(Database.Session,
					               saveResult =>
					               	{
					               		throw new NotImplementedException();
					               		//var success = false;

					               		//foreach (var response in saveResult.GetSaveResponses())
					               		//{
					               		//    success = response.Data.Equals(_document.JsonDocument);
					               		//    if (success)
					               		//    {
					               		//        Id = _document.Id;
					               		//        break;
					               		//    }
					               		//}
					               		////TO DO
					               		//if (!success)
					               		//{
					               		//    WindowManager.ShowDialog(new InformationDialogViewModel("Error", ""), null);
					               		//}
					               	});
				}
				else
				{
					WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document Metadata)",
					                                                        Document.ParseExceptionMessage));
				}
			}
			else
			{
				WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document)", Document.ParseExceptionMessage));
			}
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