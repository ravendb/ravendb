using System;
using System.Diagnostics;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Data;

namespace Raven.Client.Document.SessionOperations
{
    public class LoadOperation
    {
#if !DNXCORE50
        private readonly static ILog log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog log = LogManager.GetLogger(typeof(LoadOperation));
#endif

        protected readonly InMemoryDocumentSessionOperations sessionOperations;
        private readonly Func<IDisposable> disableAllCaching;
        protected readonly string id;
        private bool firstRequest = true;
        protected JsonDocument documentFound;

        private Stopwatch sp;

        public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id)
        {
            if (id == null) throw new ArgumentNullException("id", "The document id cannot be null");
            this.sessionOperations = sessionOperations;
            this.disableAllCaching = disableAllCaching;
            this.id = id;
        }

        public void LogOperation()
        {
            if (log.IsDebugEnabled)
                log.Debug("Loading document [{0}] from {1}", id, sessionOperations.StoreIdentifier);
            }			

        public IDisposable EnterLoadContext()
        {
            if (firstRequest == false) // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
                return disableAllCaching();
            sp = Stopwatch.StartNew();
            return null;
        }

        public bool SetResult(JsonDocument document)
        {
            firstRequest = false;
            documentFound = document;
            if (documentFound == null)
                return false;

            return
                documentFound.NonAuthoritativeInformation.HasValue &&
                documentFound.NonAuthoritativeInformation.Value &&
                sessionOperations.AllowNonAuthoritativeInformation == false &&
                sp.Elapsed < sessionOperations.NonAuthoritativeInformationTimeout;
        }

        public virtual T Complete<T>()
        {
            if (documentFound == null)
            {
                sessionOperations.RegisterMissing(id);
                return default(T);
            }
            return sessionOperations.TrackEntity<T>(documentFound);
        }
    }
}
