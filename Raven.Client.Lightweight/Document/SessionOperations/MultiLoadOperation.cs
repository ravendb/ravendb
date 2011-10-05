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
		internal Func<IDisposable> disableAllCaching { get; set; }
		internal string[] ids { get; set; }
		bool firstRequest = true;
		JsonDocument[] results;
		JsonDocument[] includeResults;
				
		private Stopwatch sp;

		public MultiLoadOperation(InMemoryDocumentSessionOperations sessionOperations)
		{
			this.sessionOperations = sessionOperations;
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

			return	sessionOperations.AllowNonAuthoritiveInformation == false &&
					results.Any(x => x.NonAuthoritiveInformation ?? false) &&
					sp.Elapsed < sessionOperations.NonAuthoritiveInformationTimeout
				;
		}

		public T[] Complete<T>()
		{
			foreach (var include in includeResults)
			{
				sessionOperations.TrackEntity<object>(include);
			}

			return results
				.Select(sessionOperations.TrackEntity<T>)
				.ToArray();
		}
	}
}