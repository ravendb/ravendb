using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text;
using System.Windows;
using System.Windows.Markup;
using Caliburn.Micro;
using Newtonsoft.Json.Linq;
using Raven.ManagementStudio.Plugin;
using Raven.ManagementStudio.UI.Silverlight.Controls;
using Raven.ManagementStudio.UI.Silverlight.Dialogs;
using Raven.ManagementStudio.UI.Silverlight.Messages;
using Raven.ManagementStudio.UI.Silverlight.Models;
using Raven.ManagementStudio.UI.Silverlight.Plugins.Documents.Browse;

namespace Raven.ManagementStudio.UI.Silverlight.Plugins.Common
{
    public class DocumentViewModel : Screen, IRavenScreen
    {
        private static readonly HashSet<string> FilteredMetadataKeys = new HashSet<string>
                                                                           {
                                                                               "@id",
                                                                               "@etag",
                                                                               "Non-Authoritive-Information",
                                                                               "Last-Modified"
                                                                           };
        private readonly Document _document;
        private bool _isSelected;
        private string _jsonData;
        private string _jsonMetadata;
        private string _id;

        public DocumentViewModel(Document document, IDatabase database, IRavenScreen parent)
        {
            DisplayName = "Edit Document";
            _document = document;
            _id = document.Id;
            _jsonData = PrepareRawJsonString(document.Data, false);
            _jsonMetadata = PrepareRawJsonString(document.Metadata, true);
            Thumbnail = new DocumentThumbnail();
            ParentRavenScreen = parent;
            Database = database;
            CompositionInitializer.SatisfyImports(this);
            CustomizedThumbnailTemplate = CreateThumbnailTemplate(document.Metadata);
        }

        public DocumentThumbnail Thumbnail { get; set; }

        public FrameworkElement CustomizedThumbnailTemplate { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public SectionType Section
        {
            get { return SectionType.Documents; }
        }

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

        public const int SummaryLength = 150;

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
            get
            {
                return _isSelected;
            }

            set
            {
                _isSelected = value;
                NotifyOfPropertyChange(() => IsSelected);
            }
        }

        public void SelectUnselect()
        {
            IsSelected = !IsSelected;
        }

        public void Preview()
        {
            var documentScreen = (DocumentsScreenViewModel)ParentRavenScreen;
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
                    WindowManager.ShowDialog(new InformationDialogViewModel("Invalid JSON (Document Metadata)", Document.ParseExceptionMessage));
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

        private static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data, bool withFilter)
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

        private static FrameworkElement CreateThumbnailTemplate(IDictionary<string, JToken> metadata)
        {
            FrameworkElement thumbnailTemplate;

            if (metadata.ContainsKey("Raven-View-Template"))
            {
                var data = metadata["Raven-View-Template"].ToString();

                var xaml = data.Substring(data.IndexOf('"') + 1, data.LastIndexOf('"') - 1);

                thumbnailTemplate = (FrameworkElement)XamlReader.Load(xaml);
            }
            else
            {
                thumbnailTemplate = (FrameworkElement)XamlReader.Load(@"
                <Border xmlns='http://schemas.microsoft.com/winfx/2006/xaml/presentation' 
                        Height='150' Width='110' BorderBrush='LightGray' BorderThickness='1'>
                    <TextBlock Text='{Binding JsonData}' TextWrapping='Wrap' />
                </Border>");
            }

            return thumbnailTemplate;
        }
    }
}
