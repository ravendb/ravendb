namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System.Collections.Generic;
    using System.Linq;
    using Caliburn.Micro;
    using Raven.Database.Indexing;

    public class Index : PropertyChangedBase
    {
        private bool isEdited;
        private string name;

        public Index(KeyValuePair<string, IndexDefinition> index)
        {
            this.Name = index.Key;
            this.CurrentName = index.Key;
            this.Definition = index.Value;

            this.PrepareFieldStoresAndIndexes();
        }

        public string Name
        {
            get { return this.name; }
            set
            {
                this.name = value;
                this.NotifyOfPropertyChange(() => this.Name);
            }
        }

        public IObservableCollection<FieldStorageAndIndexing> FieldStoresAndIndexes { get; set; }

        public string CurrentName { get; set; }

        public string Map
        {
            get { return this.Definition.Map; }
            set { this.Definition.Map = value; }
        }

        public IndexDefinition Definition { get; set; }

        public bool IsEdited
        {
            get { return this.isEdited; }
            set
            {
                this.isEdited = value;
                this.NotifyOfPropertyChange(() => this.IsEdited);
            }
        }

        private void PrepareFieldStoresAndIndexes()
        {
            this.FieldStoresAndIndexes = new BindableCollection<FieldStorageAndIndexing>();

            foreach (string key in this.Definition.Stores.Keys)
            {
                this.FieldStoresAndIndexes.Add(new FieldStorageAndIndexing
                                                   {
                                                       FieldName = key,
                                                       FieldIndexing = this.Definition.Indexes[key],
                                                       FieldStorage = this.Definition.Stores[key]
                                                   });
            }
        }

        public void AddFieldStorageAndIndexing()
        {
            if (!this.FieldStoresAndIndexes.Any(x => string.IsNullOrEmpty(x.FieldName)))
            {
                this.FieldStoresAndIndexes.Add(new FieldStorageAndIndexing());
            }
        }

        #region Nested type: FieldStorageAndIndexing

        public class FieldStorageAndIndexing
        {
            public string FieldName { get; set; }

            public FieldStorage FieldStorage { get; set; }

            public FieldIndexing FieldIndexing { get; set; }
        }

        #endregion
    }
}