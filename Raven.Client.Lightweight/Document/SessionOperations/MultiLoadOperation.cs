using System;
using System.Diagnostics;
using System.Linq;
using NLog;
using Raven.Abstractions.Data;
using Raven.Client.Connection;

namespace Raven.Client.Document.SessionOperations
{
	public class MultiLoadOperation
	{
		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly InMemoryDocumentSessionOperations sessionOperations;
		private readonly Func<IDisposable> disableAllCaching;
		private readonly string[] ids;
		bool firstRequest = true;
		JsonDocument[] results;
		JsonDocument[] includeResults;

		private Stopwatch sp;

		public MultiLoadOperation(InMemoryDocumentSessionOperations sessionOperations,
			Func<IDisposable> disableAllCaching)
			: this(sessionOperations, disableAllCaching, null)
		{

		}

		public MultiLoadOperation(InMemoryDocumentSessionOperations sessionOperations,
			Func<IDisposable> disableAllCaching,
			string[] ids)
		{
			this.sessionOperations = sessionOperations;
			this.disableAllCaching = disableAllCaching;
			this.ids = ids;


		}

		public void LogOperation()
		{
			if (ids == null)
				return;

			log.Debug("Bulk loading ids [{0}] from {1}", string.Join(", ", ids), sessionOperations.StoreIdentifier);
		}

		public IDisposable EnterMultiLoadContext()
		{
			if (firstRequest == false) // if this is a repeated request, we mustn't use the cached result, but have to re-query the server
				return disableAllCaching();
			sp = Stopwatch.StartNew();

			return null;
		}

		public bool SetResult(MultiLoadResult multiLoadResult)
		{
			firstRequest = false;
			includeResults = SerializationHelper.RavenJObjectsToJsonDocuments(multiLoadResult.Includes).ToArray();
			results = SerializationHelper.RavenJObjectsToJsonDocuments(multiLoadResult.Results).ToArray();

			return sessionOperations.AllowNonAuthoritativeInformation == false &&
					results.Where(x => x != null).Any(x => x.NonAuthoritativeInformation ?? false) &&
					sp.Elapsed < sessionOperations.NonAuthoritativeInformationTimeout
				;
		}

		public T[] Complete<T>()
		{
			foreach (var include in includeResults)
			{
				sessionOperations.TrackEntity<object>(include);
			}

			return results
				.Select(document => document == null ? default(T) : sessionOperations.TrackEntity<T>(document))
				.ToArray();
		}
	}
}