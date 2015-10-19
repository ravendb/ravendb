// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerFileSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database.Impl.Streams;

namespace Raven.Smuggler.Database.Impl.Files
{
	public class DatabaseSmugglerFileSource : IDatabaseSmugglerSource
	{
		private readonly string _path;

		private readonly CancellationToken _cancellationToken;

		private readonly List<IDatabaseSmugglerSource> _sources;

		public string DisplayName
		{
			get
			{
				throw new NotSupportedException();
			}
		}

		public bool SupportsMultipleSources => true;

		public IReadOnlyList<IDatabaseSmugglerSource> Sources => _sources;

		public DatabaseSmugglerFileSource(string path, CancellationToken cancellationToken)
		{
			_path = path;
			_cancellationToken = cancellationToken;
			_sources = new List<IDatabaseSmugglerSource>();
		}

		public void Dispose()
		{
			foreach (var source in _sources)
				source.Dispose();
		}

		public void Initialize(DatabaseSmugglerOptions options)
		{
			if (File.Exists(_path))
			{
				_sources.Add(CreateSource(options, _path, _cancellationToken));
				return;
			}

			var files = Directory.GetFiles(Path.GetFullPath(_path))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

			var optionsWithoutIndexesAndTransformers = options.Clone();
			optionsWithoutIndexesAndTransformers.OperateOnTypes &= ~(ItemType.Indexes | ItemType.Transformers);

			for (var i = 0; i < files.Length - 1; i++)
			{
				var path = Path.Combine(_path, files[i]);
				_sources.Add(CreateSource(optionsWithoutIndexesAndTransformers, path, _cancellationToken));
			}

			_sources.Add(CreateSource(options, Path.Combine(_path, files.Last()), _cancellationToken));
		}

		public Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize)
		{
			throw new NotSupportedException();
		}

		public Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync()
		{
			throw new NotSupportedException();
		}

		public Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize)
		{
			throw new NotSupportedException();
		}

		public Task<RavenJObject> ReadDocumentAsync(string key)
		{
			throw new NotSupportedException();
		}

		public Task<DatabaseStatistics> GetStatisticsAsync()
		{
			throw new NotSupportedException();
		}

		public bool SupportsReadingDatabaseStatistics => false;

		public bool SupportsReadingHiLoDocuments => false;

		public bool SupportsDocumentDeletions => false;

		public bool SupportsPaging => false;

		public bool SupportsRetries => false;

		public Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize)
		{
			throw new NotSupportedException();
		}

		public Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag)
		{
			throw new NotSupportedException();
		}

		public Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync()
		{
			throw new NotSupportedException();
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync()
		{
			throw new NotSupportedException();
		}

		private static IDatabaseSmugglerSource CreateSource(DatabaseSmugglerOptions options, string path, CancellationToken cancellationToken)
		{
			var source = new DatabaseSmugglerStreamSource(File.OpenRead(path), cancellationToken)
			{
				DisplayName = Path.GetFileName(path)
			};

			source.Initialize(options);

			return source;
		}
	}
}