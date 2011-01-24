namespace Raven.Management.Client.Silverlight
{
    using System;
    using Document;

    /// <summary>
    /// Interface for managing access to RavenDB and open sessions.
    /// </summary>
    public interface IDocumentStore : IDisposable
    {
        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        string Identifier { get; set; }

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        DocumentConvention Conventions { get; }

        /// <summary>
        /// Occurs when an entity is stored inside any session opened from this instance
        /// </summary>
        event EventHandler<StoredEntityEventArgs> Stored;

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        IDocumentStore Initialize();
    }
}