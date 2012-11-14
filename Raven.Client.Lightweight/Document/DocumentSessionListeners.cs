using Raven.Client.Listeners;

namespace Raven.Client.Document
{
	/// <summary>
	/// Holder for all the listeners for the session
	/// </summary>
	public class DocumentSessionListeners
	{
		/// <summary>
		/// Create a new instance of this class
		/// </summary>
		public DocumentSessionListeners()
		{
			ConversionListeners = new IDocumentConversionListener[0];
			QueryListeners = new IDocumentQueryListener[0];
			StoreListeners = new IDocumentStoreListener[0];
			DeleteListeners = new IDocumentDeleteListener[0];
			ConflictListeners = new IDocumentConflictListener[0];
		}

		/// <summary>
		/// The conversion listeners
		/// </summary>
		public IDocumentConversionListener[] ConversionListeners { get; set; }
		/// <summary>
		/// The query listeners allow to modify queries before it is executed
		/// </summary>
		public IDocumentQueryListener[] QueryListeners { get; set; }
		/// <summary>
		/// The store listeners
		/// </summary>
		public IDocumentStoreListener[] StoreListeners { get; set; }
		/// <summary>
		/// The delete listeners
		/// </summary>
		public IDocumentDeleteListener[] DeleteListeners { get; set; }

		/// <summary>
		/// The conflict listeners
		/// </summary>
		public IDocumentConflictListener[] ConflictListeners { get; set; }
	}
}