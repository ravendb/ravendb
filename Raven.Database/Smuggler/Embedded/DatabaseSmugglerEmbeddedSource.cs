// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerEmbeddedSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
	public class DatabaseSmugglerEmbeddedSource : DatabaseSmugglerEmbeddedBase, IDatabaseSmugglerSource
	{
		private readonly DocumentDatabase _database;

		private DatabaseSmugglerOptions _options;

		private readonly List<SmuggleType> _types = new List<SmuggleType>
		{
			SmuggleType.Index,
			SmuggleType.Document,
            SmuggleType.DocumentDeletion,
			SmuggleType.Transformer,
			SmuggleType.Identity,
			SmuggleType.None
		};

		private int _typeIndex;

		public DatabaseSmugglerEmbeddedSource(DocumentDatabase database)
		{
			_database = database;
		}

		public void Dispose()
		{
		}

		public string DisplayName => _database.Name ?? Constants.SystemDatabase;

		public bool SupportsMultipleSources => false;

		public IReadOnlyList<IDatabaseSmugglerSource> Sources => null;

		public async Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
		    _typeIndex = 0;
			_options = options;

            await InitializeBatchSizeAsync(_database, _options).ConfigureAwait(false);
		}

		public Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, CancellationToken cancellationToken)
		{
			var indexes = _database
				.IndexDefinitionStorage
				.IndexNames
				.Skip(start)
				.Take(pageSize)
				.Select(x => _database.IndexDefinitionStorage.GetIndexDefinition(x))
				.ToList();

			return new CompletedTask<List<IndexDefinition>>(indexes);
		}

		public Task<DatabaseLastEtagsInfo> FetchCurrentMaxEtagsAsync(CancellationToken cancellationToken)
		{
			DatabaseLastEtagsInfo result = null;
			_database.TransactionalStorage.Batch(accessor =>
			{
				result = new DatabaseLastEtagsInfo
				{
					LastDocsEtag = accessor.Staleness.GetMostRecentDocumentEtag()
				};

				var lastDocumentTombstone = accessor.Lists.ReadLast(Constants.RavenPeriodicExportsDocsTombstones);
				if (lastDocumentTombstone != null)
					result.LastDocDeleteEtag = lastDocumentTombstone.Etag;
			});

			return new CompletedTask<DatabaseLastEtagsInfo>(result);
		}

		public Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAfterAsync(Etag afterEtag, int pageSize, CancellationToken cancellationToken)
		{
			const int dummy = 0;
			var enumerator = _database.Documents.GetDocumentsAsJson(dummy, Math.Min(_options.BatchSize, pageSize), afterEtag, CancellationToken.None)
				.ToList()
				.Cast<RavenJObject>()
				.GetEnumerator();

			return new CompletedTask<IAsyncEnumerator<RavenJObject>>(new AsyncEnumeratorBridge<RavenJObject>(enumerator));
		}

		public Task<RavenJObject> ReadDocumentAsync(string key, CancellationToken cancellationToken)
		{
		    var document = _database.Documents.Get(key);
            if (document == null)
                return new CompletedTask<RavenJObject>((RavenJObject)null);

            JsonDocument.EnsureIdInMetadata(document);

            return new CompletedTask<RavenJObject>(document.ToJson());
		}

		public Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public bool SupportsReadingDatabaseStatistics => false;

		public bool SupportsReadingHiLoDocuments => true;

		public bool SupportsDocumentDeletions => true;

		public bool SupportsPaging => true;

		public bool SupportsRetries => true;

		public Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int pageSize, CancellationToken cancellationToken)
		{
			var transformers = _database
				.IndexDefinitionStorage
				.TransformerNames
				.Skip(start)
				.Take(pageSize)
				.Select(x => _database.IndexDefinitionStorage.GetTransformerDefinition(x))
				.ToList();

			return new CompletedTask<List<TransformerDefinition>>(transformers);
		}

		public Task<List<KeyValuePair<string, Etag>>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag, CancellationToken cancellationToken)
		{
			var results = new List<KeyValuePair<string, Etag>>();
			_database.TransactionalStorage.Batch(accessor =>
			{
				foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, fromEtag, maxEtag, int.MaxValue))
					results.Add(new KeyValuePair<string, Etag>(listItem.Key, listItem.Etag));
			});

			return new CompletedTask<List<KeyValuePair<string, Etag>>>(results);
		}

		public Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(CancellationToken cancellationToken)
		{
			var start = 0;
			const int PageSize = 1024;

			long totalCount = 0;
			var identities = new List<KeyValuePair<string, long>>();

			do
			{
				_database.TransactionalStorage.Batch(accessor => identities.AddRange(accessor.General.GetIdentities(start, PageSize, out totalCount)));
				start += PageSize;
			} while (identities.Count < totalCount);

			return new CompletedTask<List<KeyValuePair<string, long>>>(identities);
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask<SmuggleType>(_types[_typeIndex++]);
		}

		public Task SkipDocumentsAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipIndexesAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipTransformersAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipDocumentDeletionsAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipIdentitiesAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipAttachmentsAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

		public Task SkipAttachmentDeletionsAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask();
		}

	    public Task AfterExecuteAsync(DatabaseSmugglerOperationState state)
	    {
            return new CompletedTask();
        }

	    public void OnException(SmugglerException exception)
	    {
	    }
	}
}