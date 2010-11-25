namespace Raven.Client.Silverlight.Index
{
    using System;
    using Raven.Client.Silverlight.Common.Helpers;

    public class IndexStore
    {
        public IndexStore(string databaseAddress)
        {
            this.DatabaseAddress = new Uri(databaseAddress);
        }

        private Uri DatabaseAddress { get; set; }

        public IAsyncIndexSession OpenAsyncSession()
        {
            Guard.Assert(() => this.DatabaseAddress != null);

            return new IndexSession(this.DatabaseAddress);
        }
    }
}
