namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.ComponentModel.Composition;
    using System.ComponentModel.Composition.Hosting;
    using System.Linq;
    using Client.Document;
    using Management.Client.Silverlight;
    using Management.Client.Silverlight.Attachments;
    using Management.Client.Silverlight.Common;
    using Plugin;
    using Raven.Database.Data;

    public class Database : IDatabase, INotifyPropertyChanged
    {
        private bool isBusy;

        public Database(string databaseAdress, string databaseName = null)
        {
            Address = databaseAdress;
            Name = databaseName ?? databaseAdress;
            InitializeSession();
        }

        [ImportMany(AllowRecomposition = true)]
        public IList<IPlugin> Plugins { get; set; }

        public bool IsBusy
        {
            get { return isBusy; }
            set
            {
                isBusy = value;
                NotifyPropertyChange("IsBusy");
            }
        }

        public IAsyncAttachmentSession AttachmentSession { get; set; }

        #region IDatabase Members

        public string Address { get; set; }

        public string Name { get; set; }

        public IAsyncDocumentSession Session { get; set; }

        #endregion

        #region INotifyPropertyChanged Members

        public event PropertyChangedEventHandler PropertyChanged;

        #endregion

        private void NotifyPropertyChange(string propertyName)
        {
            if (PropertyChanged != null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
            }
        }

        private void InitializeSession()
        {
            var store = new DocumentStore
                            {
                                Url = Address
                            };

            store.Initialize();
            Session = store.OpenAsyncSession();

            AttachmentSession = new AsyncAttachmentSession(Address);
        }

        private void DownloadPlugins(LoadResponse<IList<KeyValuePair<string, Attachment>>> response)
        {
            if (response.IsSuccess)
            {
                int count = 0;

                foreach (DeploymentCatalog deploymentCatalog in response.Data.Select(plugin => new DeploymentCatalog(new Uri(string.Format(DatabaseUrl.Attachment, Address, plugin.Key)))))
                {
                    deploymentCatalog.DownloadCompleted += (s, e) =>
                                                               {
                                                                   if (!e.Cancelled && e.Error == null)
                                                                   {
                                                                       var catalog = s as DeploymentCatalog;
                                                                   }

                                                                   count++;
                                                                   if (count == response.Data.Count)
                                                                   {
                                                                       IsBusy = false;
                                                                   }
                                                               };

                    deploymentCatalog.DownloadAsync();
                }

                if (count == response.Data.Count)
                {
                    IsBusy = false;
                }
            }
        }
    }
}