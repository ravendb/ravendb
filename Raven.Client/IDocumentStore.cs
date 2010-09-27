using System;
using System.Collections.Specialized;
using Raven.Client.Client;
using Raven.Client.Document;

namespace Raven.Client
{
	/// <summary>
	/// Interface for managing access to RavenDB and open sessions.
	/// </summary>
    public interface IDocumentStore : IDisposable
    {
		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
		NameValueCollection SharedOperationsHeaders { get; }

		/// <summary>
		/// Occurs when an entity is stored inside any session opened from this instance
		/// </summary>
		event EventHandler<StoredEntityEventArgs> Stored;

		/// <summary>
		/// Gets or sets the identifier for this store.
		/// </summary>
		/// <value>The identifier.</value>
		string Identifier { get; set; }

		/// <summary>
		/// Initializes this instance.
		/// </summary>
		/// <returns></returns>
        IDocumentStore Initialize();

		/// <summary>
		/// Registers the delete listener.
		/// </summary>
		/// <param name="deleteListener">The delete listener.</param>
		/// <returns></returns>
    	IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener);

		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener);

		/// <summary>
		/// Opens the session.
		/// </summary>
		/// <returns></returns>
        IDocumentSession OpenSession();

		/// <summary>
		/// Gets the database commands.
		/// </summary>
		/// <value>The database commands.</value>
        IDatabaseCommands DatabaseCommands { get; }

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
    	DocumentConvention Conventions { get; }
    }

	/// <summary>
	/// The event args raised when an entity is stored
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
