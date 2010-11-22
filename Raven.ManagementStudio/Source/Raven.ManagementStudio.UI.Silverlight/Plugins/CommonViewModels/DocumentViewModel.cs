namespace Raven.ManagementStudio.UI.Silverlight.Plugins.CommonViewModels
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Text;
    using System.Windows;
    using System.Windows.Controls;
    using System.Windows.Markup;
    using Caliburn.Micro;
    using Controls;
    using Documents.Browse;
    using Messages;
    using Models;
    using Newtonsoft.Json.Linq;
    using Plugin;

    public class DocumentViewModel : Screen, IRavenScreen
    {
        private readonly Document document;
        private bool isSelected;
        private string jsonData;
        private string jsonMetadata;

        public DocumentViewModel(Document document, IDatabase database, IRavenScreen parent)
        {
            this.DisplayName = "Doc";
            this.document = document;
            this.jsonData = PrepareRawJsonString(document.Data);
            this.jsonMetadata = PrepareRawJsonString(document.Metadata);
            this.Thumbnail = new DocumentThumbnail();
            this.ParentRavenScreen = parent;
            this.Database = database;
            CompositionInitializer.SatisfyImports(this);
            this.CustomizedThumbnailTemplate = CreateThumbnailTemplate(document.Metadata);
        }

        public DocumentThumbnail Thumbnail { get; set; }

        public FrameworkElement CustomizedThumbnailTemplate { get; set; }

        public IRavenScreen ParentRavenScreen { get; set; }

        public string Id
        {
            get { return this.document.Id; }

            set
            {
                this.document.Id = value;
                NotifyOfPropertyChange(() => this.Id);
            }
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public IDatabase Database { get; set; }

        public string JsonData
        {
            get
            {
                return this.jsonData;
            }

            set
            {
                this.jsonData = value;
                this.document.SetData(jsonData);
                NotifyOfPropertyChange(() => this.JsonData);
            }
        }

        public string JsonMetadata
        {
            get
            {
                return this.jsonMetadata;
            }

            set
            {
                this.jsonMetadata = value;
                NotifyOfPropertyChange(() => this.JsonMetadata);
            }
        }

        public bool IsSelected
        {
            get 
            { 
                return this.isSelected; 
            }

            set
            {
                this.isSelected = value; 
                NotifyOfPropertyChange(() => this.IsSelected);
            }
        }

        public void SelectUnselect()
        {
            this.IsSelected = !this.IsSelected;
        }

        public void Preview()
        {
            var documentScreen = (DocumentsScreenViewModel)this.ParentRavenScreen;
            documentScreen.ActivateItem(this);
            documentScreen.IsDocumentPreviewed = true; 
        }

        public void Edit()
        {
            var view = this.GetView(null) as Control;
            if (view != null)
            {
                VisualStateManager.GoToState(view, "EditState", false);
            }
            else
            {
                this.EventAggregator.Publish(new ReplaceActiveScreen(this));
                view = this.GetView(null) as Control;
                if (view != null)
                {
                    VisualStateManager.GoToState(view, "EditState", false);
                }
            }
        }

        public void Save()
        {
            var view = this.GetView(null) as Control;
            if (view != null)
            {
                //TO DO
                this.document.SetData(this.JsonData);
                this.document.Save(this.Database.Session);
                VisualStateManager.GoToState(view, "NormalState", false);
            }
        }

        public void Cancel()
        {
            var view = this.GetView(null) as Control;
            if (view != null)
            {
                VisualStateManager.GoToState(view, "NormalState", false);
            }
        }

        public void ShowDocument()
        {
            this.EventAggregator.Publish(new ReplaceActiveScreen(this));
        }

        private static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data)
        {
            var result = new StringBuilder("{\n");

            foreach (var item in data)
            {
                result.Append("\"").Append(item.Key).Append("\" : ").Append(item.Value).Append(",\n");
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