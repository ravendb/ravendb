using System;
using System.Linq;
using Raven.Client.Listeners;

namespace Raven.Client.Document
{
    /// <summary>
    ///     Holder for all the listeners for the session
    /// </summary>
    public class DocumentSessionListeners
    {
        private bool closed;

        /// <summary>
        ///     Create a new instance of this class
        /// </summary>
        public DocumentSessionListeners()
        {
            ConversionListeners = new IDocumentConversionListener[0];
            ExtendedConversionListeners = new IExtendedDocumentConversionListener[0];
            QueryListeners = new IDocumentQueryListener[0];
            StoreListeners = new IDocumentStoreListener[0];
            DeleteListeners = new IDocumentDeleteListener[0];
            ConflictListeners = new IDocumentConflictListener[0];
        }

        /// <summary>
        ///     The conversion listeners
        /// </summary>
        public IDocumentConversionListener[] ConversionListeners { get; set; }
        /// <summary>
        ///     The extended conversion listeners
        /// </summary>
        public IExtendedDocumentConversionListener[] ExtendedConversionListeners { get; set; }
        /// <summary>
        ///     The query listeners allow to modify queries before it is executed
        /// </summary>
        public IDocumentQueryListener[] QueryListeners { get; set; }
        /// <summary>
        ///     The store listeners
        /// </summary>
        public IDocumentStoreListener[] StoreListeners { get; set; }
        /// <summary>
        ///     The delete listeners
        /// </summary>
        public IDocumentDeleteListener[] DeleteListeners { get; set; }

        /// <summary>
        ///     The conflict listeners
        /// </summary>
        public IDocumentConflictListener[] ConflictListeners { get; set; }

        public void RegisterListener(IDocumentConversionListener conversionListener)
        {
            if(closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            ConversionListeners = ConversionListeners.Concat(new[] {conversionListener}).ToArray();
        }


        public void RegisterListener(IExtendedDocumentConversionListener conversionListener)
        {
            if (closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            ExtendedConversionListeners = ExtendedConversionListeners.Concat(new[] { conversionListener }).ToArray();
        }


        public void RegisterListener(IDocumentQueryListener conversionListener)
        {
            if (closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            QueryListeners = QueryListeners.Concat(new[] { conversionListener }).ToArray();
        }


        public void RegisterListener(IDocumentStoreListener conversionListener)
        {
            if (closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            StoreListeners = StoreListeners.Concat(new[] { conversionListener }).ToArray();
        }


        public void RegisterListener(IDocumentDeleteListener conversionListener)
        {
            if (closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            DeleteListeners = DeleteListeners.Concat(new[] { conversionListener }).ToArray();
        }


        public void RegisterListener(IDocumentConflictListener conversionListener)
        {
            if (closed)
                throw new InvalidOperationException("Cannot modify after listeners were closed");
            ConflictListeners = ConflictListeners.Concat(new[] { conversionListener }).ToArray();
        }

        public void SetFrom(DocumentSessionListeners other)
        {
            other.closed = true;
            DeleteListeners = other.DeleteListeners;
            QueryListeners = other.QueryListeners;
            ConversionListeners = other.ConversionListeners;
            ExtendedConversionListeners = other.ExtendedConversionListeners;
            StoreListeners = other.StoreListeners;
            ConflictListeners = other.ConflictListeners;
        }
    }
}