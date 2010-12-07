using System;
using System.Net;
using Raven.Abstractions.Data;
using Raven.Management.Client.Silverlight;
using Raven.Management.Client.Silverlight.Client;
using Raven.Management.Client.Silverlight.Document;

namespace Raven.Client.Document
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : IDocumentStore
    {
        private Func<IAsyncDatabaseCommands> _asyncDatabaseCommandsGenerator;
        private ICredentials _credentials;
        private string _identifier;

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentStore"/> class.
        /// </summary>
        public DocumentStore()
        {
            ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");

            SharedOperationsHeaders = new NameValueCollection();
            Conventions = new DocumentConvention();
        }

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        public NameValueCollection SharedOperationsHeaders { get; private set; }

        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return _asyncDatabaseCommandsGenerator == null ? null : _asyncDatabaseCommandsGenerator(); }
        }

        /// <summary>
        /// Gets or sets the credentials.
        /// </summary>
        /// <value>The credentials.</value>
        public ICredentials Credentials
        {
            get { return _credentials; }
            set { _credentials = value; }
        }

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        /// <value>The URL.</value>
        public string Url { get; set; }

        /// <summary>
        /// The resource manager id for the document store.
        /// IMPORTANT: Using Guid.NewGuid() to set this value is almost certainly a mistake, you should set
        /// it to a value that remains consistent between restart of the system.
        /// </summary>
        public Guid ResourceManagerId { get; set; }

        #region IDocumentStore Members

        /// <summary>
        /// Occurs when an entity is stored inside any session opened from this instance
        /// </summary>
        public event EventHandler<StoredEntityEventArgs> Stored;

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public virtual string Identifier
        {
            get { return _identifier ?? Url; }
            set { _identifier = value; }
        }

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public DocumentConvention Conventions { get; set; }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
            Stored = null;
        }


        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        public IDocumentStore Initialize()
        {
            try
            {
                InitializeInternal();
                if (Conventions.DocumentKeyGenerator == null) // don't overwrite what the user is doing
                {
                    Conventions.DocumentKeyGenerator = entity => { return Guid.NewGuid().ToString(); };
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        #endregion

        private void OnSessionStored(object entity)
        {
            EventHandler<StoredEntityEventArgs> copy = Stored;
            if (copy != null)
                copy(this, new StoredEntityEventArgs
                               {
                                   SessionIdentifier = Identifier,
                                   EntityInstance = entity
                               });
        }

        /// <summary>
        /// Initialize the document store access method to RavenDB
        /// </summary>
        protected virtual void InitializeInternal()
        {
            _asyncDatabaseCommandsGenerator = () => new AsyncServerClient(Url, Conventions, _credentials);
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public IAsyncDocumentSession OpenAsyncSession()
        {
            if (AsyncDatabaseCommands == null)
                throw new InvalidOperationException(
                    "You cannot open an async session because it is not supported on embedded mode");

            var session = new AsyncDocumentSession(this);
            session.Stored += OnSessionStored;
            return session;
        }
    }
}
