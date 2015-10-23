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
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.Database.Streams;

namespace Raven.Smuggler.Database.Files
{
	public class DatabaseSmugglerFileSource : IDatabaseSmugglerSource
	{
		private readonly string _path;

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

		public DatabaseSmugglerFileSource(string fileOrDirectoryPath)
		{
			_path = fileOrDirectoryPath;
			_sources = new List<IDatabaseSmugglerSource>();
		}

		public void Dispose()
		{
			foreach (var source in _sources)
				source.Dispose();
		}

		public async Task InitializeAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			if (File.Exists(_path))
			{
				_sources.Add(await CreateSourceAsync(options, _path, cancellationToken).ConfigureAwait(false));
				return;
			}

			var files = Directory.GetFiles(Path.GetFullPath(_path))
				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
				.OrderBy(File.GetLastWriteTimeUtc)
				.ToArray();

			if (files.Length == 0)
				return;

			var optionsWithoutIndexesAndTransformers = options.Clone();
			optionsWithoutIndexesAndTransformers.OperateOnTypes &= ~(DatabaseItemType.Indexes | DatabaseItemType.Transformers);

			for (var i = 0; i < files.Length - 1; i++)
			{
				var path = Path.Combine(_path, files[i]);
				_sources.Add(await CreateSourceAsync(optionsWithoutIndexesAndTransformers, path, cancellationToken).ConfigureAwait(false));
			}

			_sources.Add(await CreateSourceAsync(options, Path.Combine(_path, files.Last()), cancellationToken).ConfigureAwait(false));
		}

		public Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<DatabaseLastEtagsInfo> FetchCurrentMaxEtagsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<RavenJObject> ReadDocumentAsync(string key, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public bool SupportsReadingDatabaseStatistics => false;

		public bool SupportsReadingHiLoDocuments => false;

		public bool SupportsDocumentDeletions => false;

		public bool SupportsPaging => false;

		public bool SupportsRetries => false;

		public Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipDocumentsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipIndexesAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipTransformersAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipDocumentDeletionsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipIdentitiesAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipAttachmentsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task SkipAttachmentDeletionsAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		private static async Task<IDatabaseSmugglerSource> CreateSourceAsync(DatabaseSmugglerOptions options, string path, CancellationToken cancellationToken)
		{
			var source = new DatabaseSmugglerStreamSource(File.OpenRead(path))
			{
				DisplayName = Path.GetFileName(path)
			};

			await source
				.InitializeAsync(options, cancellationToken)
				.ConfigureAwait(false);

			return source;
		}
	}
}