// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerEmbeddedSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
	public class DatabaseSmugglerEmbeddedSource : IDatabaseSmugglerSource
	{
		private readonly DocumentDatabase _database;

		public DatabaseSmugglerEmbeddedSource(DocumentDatabase database)
		{
			_database = database;
		}

		public void Dispose()
		{
			throw new System.NotImplementedException();
		}

		public string DisplayName { get; }

		public bool SupportsMultipleSources => false;

		public IReadOnlyList<IDatabaseSmugglerSource> Sources => null;

		public Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<RavenJObject> ReadDocumentAsync(string key, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public bool SupportsReadingDatabaseStatistics => false;

		public bool SupportsReadingHiLoDocuments => true;

		public bool SupportsDocumentDeletions => true;

		public bool SupportsPaging => true;

		public bool SupportsRetries => true;

		public Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag, CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SkipDocumentsAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SkipIndexesAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SkipTransformersAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SkipDocumentDeletionsAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}

		public Task SkipIdentitiesAsync(CancellationToken cancellationToken)
		{
			throw new System.NotImplementedException();
		}
	}
}