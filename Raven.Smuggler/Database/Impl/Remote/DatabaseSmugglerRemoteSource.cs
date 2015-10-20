// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerRemoteSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Remote
{
	public class DatabaseSmugglerRemoteSource : IDatabaseSmugglerSource
	{
		private readonly DocumentStore _store;

		private DatabaseSmugglerOptions _options;

		private readonly List<SmuggleType> _types = new List<SmuggleType>
		{
			SmuggleType.Index,
			SmuggleType.Document,
			SmuggleType.Transformer,
			SmuggleType.Identity,
			SmuggleType.None
		};

		private int _typeIndex;

		public DatabaseSmugglerRemoteSource(DocumentStore store)
		{
			_store = store;
		}

		public void Dispose()
		{
		}

		public string DisplayName => _store.Url;

		public bool SupportsMultipleSources => false;

		public IReadOnlyList<IDatabaseSmugglerSource> Sources => null;

		public async Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			_options = options;

			InitializeStore();
			await InitializeBatchSizeAsync().ConfigureAwait(false);
		}

		private void InitializeStore()
		{
			_store.Initialize(ensureDatabaseExists: false);
		}

		private async Task InitializeBatchSizeAsync()
		{
			if (_store.HasJsonRequestFactory == false)
				return;

			var url = _store.Url.ForDatabase(_store.DefaultDatabase) + "/debug/config";
			try
			{
				using (var request = _store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, _store.DatabaseCommands.PrimaryCredentials, _store.Conventions)))
				{
					var configuration = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);

					var maxNumberOfItemsToProcessInSingleBatch = configuration.Value<int>("MaxNumberOfItemsToProcessInSingleBatch");
					if (maxNumberOfItemsToProcessInSingleBatch <= 0)
						return;

					var current = _options.BatchSize;
					_options.BatchSize = Math.Min(current, maxNumberOfItemsToProcessInSingleBatch);
				}
			}
			catch (ErrorResponseException e)
			{
				if (e.StatusCode == HttpStatusCode.Forbidden) // let it continue with the user defined batch size
					return;

				throw;
			}
		}

		public async Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, CancellationToken cancellationToken)
		{
			var indexes = await _store
				.AsyncDatabaseCommands
				.GetIndexesAsync(start, pageSize, cancellationToken)
				.ConfigureAwait(false);

			return indexes.ToList();
		}

		public Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask<LastEtagsInfo>(new LastEtagsInfo
			{
				LastDocDeleteEtag = null,
				LastDocsEtag = null
			});
		}

		public async Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize, CancellationToken cancellationToken)
		{
			return await _store
				.AsyncDatabaseCommands
				.StreamDocsAsync(fromEtag, pageSize: pageSize, token: cancellationToken)
				.ConfigureAwait(false);

			//TODO [ppekrol] legacy support
		}

		public async Task<RavenJObject> ReadDocumentAsync(string key, CancellationToken cancellationToken)
		{
			var document = await _store
				.AsyncDatabaseCommands
				.GetAsync(key, cancellationToken)
				.ConfigureAwait(false);

			return document.ToJson();
		}

		public Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
		{
			return _store.AsyncDatabaseCommands.GetStatisticsAsync(cancellationToken);
		}

		public bool SupportsReadingDatabaseStatistics => true;

		public bool SupportsReadingHiLoDocuments => true;

		public bool SupportsDocumentDeletions => false;

		public bool SupportsPaging => true;

		public bool SupportsRetries => true;

		public async Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, CancellationToken cancellationToken)
		{
			var transformers = await _store
				.AsyncDatabaseCommands
				.GetTransformersAsync(start, batchSize, cancellationToken)
				.ConfigureAwait(false);

			return transformers.ToList();
		}

		public Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public async Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(CancellationToken cancellationToken)
		{
			// TODO [ppekrol] legacy support

			var start = 0;
			const int PageSize = 1024;
			long totalIdentitiesCount;
			var identities = new List<KeyValuePair<string, long>>();

			do
			{
				var url = _store.Url.ForDatabase(_store.DefaultDatabase) + "/debug/identities?start=" + start + "&pageSize=" + PageSize;
				using (var request = _store.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, HttpMethods.Get, _store.DatabaseCommands.PrimaryCredentials, _store.Conventions)))
				{
					var identitiesInfo = (RavenJObject)await request.ReadResponseJsonAsync().ConfigureAwait(false);
					totalIdentitiesCount = identitiesInfo.Value<long>("TotalCount");

					foreach (var identity in identitiesInfo.Value<RavenJArray>("Identities"))
						identities.Add(new KeyValuePair<string, long>(identity.Value<string>("Key"), identity.Value<long>("Value")));

					start += PageSize;
				}
			} while (identities.Count < totalIdentitiesCount);

			return identities;
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
		{
			return new CompletedTask<SmuggleType>(_types[_typeIndex++]);
		}
	}
}