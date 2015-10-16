using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerSource : IDisposable
	{
		void Initialize();

		Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, DatabaseSmugglerOptions options);

		Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync();

		Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize);

		Task<RavenJObject> ReadDocumentAsync(string key);

		Task<DatabaseStatistics> GetStatisticsAsync();

		bool SupportsGettingStatistics { get; }

		bool SupportsReadingSingleDocuments { get; }

		bool SupportsDocumentDeletions { get; }

		bool SupportsPaging { get; }

		bool SupportsRetries { get; }

		Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, DatabaseSmugglerOptions options);

		Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag);

		Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(DatabaseSmugglerOptions options);

		Task<SmuggleType> GetNextSmuggleTypeAsync();
	}
}