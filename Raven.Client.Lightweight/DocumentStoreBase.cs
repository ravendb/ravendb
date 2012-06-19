using System;
#if !SILVERLIGHT
using System.Collections.Specialized;
#endif
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Client.Document;
#if SILVERLIGHT
using Raven.Client.Silverlight.Connection;
#endif
#if !NET35
using Raven.Client.Connection.Async;
#endif

namespace Raven.Client
{
	/// <summary>
	/// Contains implementation of some IDocumentStore operations shared by DocumentStore implementations
	/// </summary>
	public abstract class DocumentStoreBase : IDocumentStore
	{
		public abstract void Dispose();
		
		/// <summary>
		/// 
		/// </summary>
		public abstract event EventHandler AfterDispose;

		/// <summary>
		/// Whatever the instance has been disposed
		/// </summary>
		public bool WasDisposed { get; protected set; }

		public abstract IDisposable AggressivelyCacheFor(TimeSpan cahceDuration);
		public abstract IDisposable DisableAggressiveCaching();
		
#if !SILVERLIGHT
		/// <summary>
		/// Gets the shared operations headers.
		/// </summary>
		/// <value>The shared operations headers.</value>
		public virtual NameValueCollection SharedOperationsHeaders { get; protected set; }
#else
		public virtual IDictionary<string,string> SharedOperationsHeaders { get; protected set; }
#endif

		public abstract HttpJsonRequestFactory JsonRequestFactory { get; }
		public abstract string Identifier { get; set; }
		public abstract IDocumentStore Initialize();
#if !NET35
		public abstract IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
		public abstract IAsyncDocumentSession OpenAsyncSession();
		public abstract IAsyncDocumentSession OpenAsyncSession(string database);
#endif
#if !SILVERLIGHT
		public abstract IDocumentSession OpenSession();
		public abstract IDocumentSession OpenSession(string database);
		public abstract IDocumentSession OpenSession(OpenSessionOptions sessionOptions);
		public abstract IDatabaseCommands DatabaseCommands { get; }

		/// <summary>
		/// Executes the index creation.
		/// </summary>
		public virtual void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
		{
			indexCreationTask.Execute(DatabaseCommands, Conventions);
		}
#endif

		private DocumentConvention conventions;

		/// <summary>
		/// Gets the conventions.
		/// </summary>
		/// <value>The conventions.</value>
		public virtual DocumentConvention Conventions
		{
			get { return conventions ?? (conventions = new DocumentConvention()); }
			set { conventions = value; }
		}

		private string url;

		/// <summary>
		/// Gets or sets the URL.
		/// </summary>
		public virtual string Url
		{
			get { return url; }
			set { url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value; }
		}

		///<summary>
		/// Whatever or not we will automatically enlist in distributed transactions
		///</summary>
		public bool EnlistInDistributedTransactions
		{
			get { return Conventions.EnlistInDistributedTransactions; }
			set { Conventions.EnlistInDistributedTransactions = value; }
		}
		/// <summary>
		/// The resource manager id for the document store.
		/// IMPORTANT: Using Guid.NewGuid() to set this value is almost certainly a mistake, you should set
		/// it to a value that remains consistent between restart of the system.
		/// </summary>
		public Guid ResourceManagerId { get; set; }

		private class EtagHolder
		{
			public Guid Etag;
			public byte[] Bytes;
		}

		private volatile EtagHolder lastEtag;
		protected readonly object lastEtagLocker = new object();
		protected bool initialized;

		internal void UpdateLastWrittenEtag(Guid? etag)
		{
			if (etag == null)
				return;

			var newEtag = etag.Value.ToByteArray();

			if (lastEtag == null)
			{
				lock (lastEtagLocker)
				{
					if (lastEtag == null)
					{
						lastEtag = new DocumentStore.EtagHolder
						{
							Bytes = newEtag,
							Etag = etag.Value
						};
						return;
					}
				}
			}

			// not the most recent etag
			if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
			{
				return;
			}

			lock (lastEtagLocker)
			{
				// not the most recent etag
				if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
				{
					return;
				}

				lastEtag = new DocumentStore.EtagHolder
				{
					Etag = etag.Value,
					Bytes = newEtag
				};
			}
		}

		///<summary>
		/// Gets the etag of the last document written by any session belonging to this 
		/// document store
		///</summary>
		public virtual Guid? GetLastWrittenEtag()
		{
			var etagHolder = lastEtag;
			if (etagHolder == null)
				return null;
			return etagHolder.Etag;
		}

		protected void EnsureNotClosed()
		{
			if (WasDisposed)
				throw new ObjectDisposedException(GetType().Name, "The document store has already been disposed and cannot be used");
		}

		protected void AssertInitialized()
		{
			if (!initialized)
				throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
		}

		protected readonly DocumentSessionListeners listeners = new DocumentSessionListeners();

		/// <summary>
		/// Registers the delete listener.
		/// </summary>
		/// <param name="deleteListener">The delete listener.</param>
		/// <returns></returns>
		public DocumentStoreBase RegisterListener(IDocumentDeleteListener deleteListener)
		{
			listeners.DeleteListeners = listeners.DeleteListeners.Concat(new[] { deleteListener }).ToArray();
			return this;
		}

		/// <summary>
		/// Registers the query listener.
		/// </summary>
		public DocumentStoreBase RegisterListener(IDocumentQueryListener queryListener)
		{
			listeners.QueryListeners = listeners.QueryListeners.Concat(new[] { queryListener }).ToArray();
			return this;
		}
		/// <summary>
		/// Registers the convertion listener.
		/// </summary>
		public DocumentStoreBase RegisterListener(IDocumentConversionListener conversionListener)
		{
			listeners.ConversionListeners = listeners.ConversionListeners.Concat(new[] { conversionListener, }).ToArray();
			return this;
		}

		protected void AfterSessionCreated(InMemoryDocumentSessionOperations session)
		{
			var onSessionCreatedInternal = SessionCreatedInternal;
			if (onSessionCreatedInternal != null)
				onSessionCreatedInternal(session);
		}

		///<summary>
		/// Internal notification for integration tools, mainly
		///</summary>
		public event Action<InMemoryDocumentSessionOperations> SessionCreatedInternal;

#if !NET35
		protected readonly ProfilingContext profilingContext = new ProfilingContext();
#endif

		/// <summary>
		/// Registers the store listener.
		/// </summary>
		/// <param name="documentStoreListener">The document store listener.</param>
		/// <returns></returns>
		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			listeners.StoreListeners = listeners.StoreListeners.Concat(new[] { documentStoreListener }).ToArray();
			return this;
		}

		/// <summary>
		///  Get the profiling information for the given id
		/// </summary>
		public ProfilingInformation GetProfilingInformationFor(Guid id)
		{
#if !NET35
			return profilingContext.TryGet(id);
#else
			return null;
#endif
		}
	}
}
