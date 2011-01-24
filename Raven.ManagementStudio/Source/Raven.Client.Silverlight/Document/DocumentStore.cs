namespace Raven.Client.Silverlight.Document
{
    using System;
    using Raven.Client.Silverlight.Common.Helpers;

    public class DocumentStore
    {
        public DocumentStore(string databaseAddress)
        {
            this.DatabaseAddress = new Uri(databaseAddress);
        }

        private Uri DatabaseAddress { get; set; }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            Guard.Assert(() => this.DatabaseAddress != null);

            return new DocumentSession(this.DatabaseAddress);
        }
    }
}
