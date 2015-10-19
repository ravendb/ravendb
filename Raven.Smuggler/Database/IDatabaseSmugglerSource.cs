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
		string DisplayName { get; }

		bool SupportsMultipleSources { get; }

		IReadOnlyList<IDatabaseSmugglerSource> Sources { get; }

		void Initialize(DatabaseSmugglerOptions options);

		Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize);

		Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync();

		Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize);

		Task<RavenJObject> ReadDocumentAsync(string key);

		Task<DatabaseStatistics> GetStatisticsAsync();

		bool SupportsReadingDatabaseStatistics { get; }

		bool SupportsReadingHiLoDocuments { get; }

		bool SupportsDocumentDeletions { get; }

		bool SupportsPaging { get; }

		bool SupportsRetries { get; }

		Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize);

		Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag);

		Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync();

		Task<SmuggleType> GetNextSmuggleTypeAsync();
	}
}