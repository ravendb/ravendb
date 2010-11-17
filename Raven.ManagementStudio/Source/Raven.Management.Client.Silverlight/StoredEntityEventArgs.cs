namespace Raven.Management.Client.Silverlight
{
    using System;

    /// <summary>
    /// The event arguments raised when an entity is stored
    /// </summary>
    public class StoredEntityEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets the session identifier.
        /// </summary>
        /// <value>The session identifier.</value>
        public string SessionIdentifier { get; set; }

        /// <summary>
        /// Gets or sets the entity instance.
        /// </summary>
        /// <value>The entity instance.</value>
        public object EntityInstance { get; set; }
    }
}