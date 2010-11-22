namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.ComponentModel.Composition;
    using Raven.Client.Document;
    using Raven.Management.Client.Silverlight;
    using Raven.Management.Client.Silverlight.Attachments;
    using Raven.Management.Client.Silverlight.Collections;
    using Raven.Management.Client.Silverlight.Indexes;
    using Raven.Management.Client.Silverlight.Statistics;
    using Raven.ManagementStudio.Plugin;

    public class Database : IDatabase, INotifyPropertyChanged
    {
        private bool isBusy;

        public Database(string databaseAdress, string databaseName = null)
        {
            this.Address = databaseAdress;
            this.Name = databaseName ?? databaseAdress;
            this.InitializeSession();
        }

        [ImportMany(AllowRecomposition = true)]
        public IList<IPlugin> Plugins { get; set; }

        public bool IsBusy
        {
            get { return this.isBusy; }
            set
            {
                this.isBusy = value;
                this.NotifyPropertyChange("IsBusy");
            }
        }

        #region IDatabase Members

        public string Address { get; set; }

        public string Name { get; set; }

        public IAsyncDocumentSession Session { get; set; }

        public IAsyncAttachmentSession AttachmentSession { get; set; }

        public IAsyncCollectionSession CollectionSession { get; set; }

        public IAsyncIndexSession IndexSession { get; set; }

        public IAsyncStatisticsSession StatisticsSession { get; set; }

        #endregion

        #region INotifyPropertyChanged Members

        public event PropertyChangedEventHandler PropertyChanged;

        #endregion

        private void NotifyPropertyChange(string propertyName)
        {
            if (this.PropertyChanged != null)
            {
                this.PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }

        private void InitializeSession()
        {
            var store = new DocumentStore
                            {
                                Url = this.Address
                            };

            store.Initialize();

            this.Session = store.OpenAsyncSession();
            this.AttachmentSession = new AsyncAttachmentSession(this.Address);
            this.CollectionSession = new AsyncCollectionSession(this.Address);
            this.IndexSession = new AsyncIndexSession(this.Address);
            this.StatisticsSession = new AsyncStatisticsSession(this.Address);

            this.AttachmentSession = new AsyncAttachmentSession(this.Address);
        }
    }
}