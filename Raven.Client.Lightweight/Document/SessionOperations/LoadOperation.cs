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
		private readonly string id;
		private bool firstRequest = true;
		private JsonDocument documentFound;

		private Stopwatch sp;

		public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, Func<IDisposable> disableAllCaching, string id)
		{
			if (id == null) throw new ArgumentNullException("id","The document id cannot be null");
			this.sessionOperations = sessionOperations;
			this.disableAllCaching = disableAllCaching;
			this.id = id;
		}

		public void LogOperation()
		{
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
				documentFound.NonAuthoritiveInformation.HasValue &&
				documentFound.NonAuthoritiveInformation.Value &&
				sessionOperations.AllowNonAuthoritiveInformation == false &&
				sp.Elapsed < sessionOperations.NonAuthoritiveInformationTimeout
				;
		}

		public T Complete<T>()
		{
			if (documentFound == null)
				return default(T);
			return sessionOperations.TrackEntity<T>(documentFound);
		}
	}
}