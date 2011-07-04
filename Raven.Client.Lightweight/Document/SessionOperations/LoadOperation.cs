using System;
using System.Diagnostics;
using System.Net;
using NLog;
using Raven.Abstractions.Data;

namespace Raven.Client.Document.SessionOperations
{
	public class LoadOperation
	{
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly InMemoryDocumentSessionOperations sessionOperations;
		private readonly Func<IDisposable> disableAllCaching;
		private bool firstRequest = true;
		private JsonDocument documentFound;

#if !SILVERLIGHT
		private Stopwatch sp;
#else
		private	DateTime startTime;
#endif

		public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id)
		{
			this.sessionOperations = sessionOperations;
			this.disableAllCaching = disableAllCaching;

			sessionOperations.IncrementRequestCount();
			log.Debug("Loading document [{0}] from {1}", id, sessionOperations.StoreIdentifier);

#if !SILVERLIGHT
			sp = Stopwatch.StartNew();
#else
			startTime = DateTime.Now;
#endif
		}

		public IDisposable EnterMLoadContext()
		{
			if (firstRequest == false) // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
				return disableAllCaching();
			return null;
		}

		public bool HandleException(WebException e)

		public bool SetResult(JsonDocument document)
		{
			firstRequest = false;
			documentFound = document;
			return
				documentFound.NonAuthoritiveInformation.HasValue &&
				documentFound.NonAuthoritiveInformation.Value &&
				sessionOperations.AllowNonAuthoritiveInformation == false &&
#if !SILVERLIGHT
				sp.Elapsed < sessionOperations.NonAuthoritiveInformationTimeout
#else
				(DateTime.Now - startTime) < sessionOperations.NonAuthoritiveInformationTimeout
#endif
				;
		}
	}
}