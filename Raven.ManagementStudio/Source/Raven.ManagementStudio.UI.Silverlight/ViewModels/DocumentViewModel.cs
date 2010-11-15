namespace Raven.ManagementStudio.UI.Silverlight.ViewModels
{
    using System.Collections.Generic;
    using System.ComponentModel.Composition;
    using System.Text;
    using Caliburn.Micro;
    using Messages;
    using Models;
    using Newtonsoft.Json.Linq;
    using Screens;

    public class DocumentViewModel : PropertyChangedBase
    {
        private Document document;
        private string jsonData;

        public DocumentViewModel(Document document)
        {
            this.Document = document;
            this.jsonData = PrepareRawJsonString(document.Data);
            CompositionInitializer.SatisfyImports(this);
        }

        public Document Document
        {
            get
            {
                return this.document;
            }
            set
            {
                NotifyOfPropertyChange(() => this.Document);
                this.document = value;
            }
        }

        [Import]
        public IEventAggregator EventAggregator { get; set; }

        public string JsonData
        {
            get { return jsonData; }
            set
            {
                jsonData = value;
                NotifyOfPropertyChange(() => this.JsonData);
            }
        }

        private static string PrepareRawJsonString(IEnumerable<KeyValuePair<string, JToken>> data)
        {
            var result = new StringBuilder("{\n");

            foreach (var item in data)
            {
                result.Append(item.Key).Append(" : ").Append(item.Value).Append("\n");
            }

            result.Append("}");

            return result.ToString();
        }

        public void GoToDocument()
        {
        }
    }
}