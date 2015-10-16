// -----------------------------------------------------------------------
//  <copyright file="IDatabaseSmugglerStreamSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Json;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamSource : IDatabaseSmugglerSource
	{
		private readonly Stream _stream;

		private readonly CancellationToken _cancellationToken;

		private CountingStream _sizeStream;

		private StreamReader _streamReader;

		private JsonTextReader _reader;

		public DatabaseSmugglerStreamSource(Stream stream, CancellationToken cancellationToken)
		{
			_stream = stream;
			_cancellationToken = cancellationToken;
		}

		public void Initialize()
		{
			try
			{
				_stream.Position = 0;
				_sizeStream = new CountingStream(new GZipStream(_stream, CompressionMode.Decompress));
				_streamReader = new StreamReader(_sizeStream);

				_reader = new RavenJsonTextReader(_streamReader);
			}
			catch (Exception e)
			{
				if (e is InvalidDataException == false)
					throw;

				_stream.Seek(0, SeekOrigin.Begin);

				_sizeStream = new CountingStream(_stream);

				_streamReader = new StreamReader(_sizeStream);

				_reader = new JsonTextReader(_streamReader);
			}

			if (_reader.Read() == false)
				return;

			if (_reader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");
		}

		public Task<List<IndexDefinition>> ReadIndexesAsync(int start, int pageSize, DatabaseSmugglerOptions options)
		{
			var results = new List<IndexDefinition>();

			while (_reader.Read() && _reader.TokenType != JsonToken.EndArray)
			{
				_cancellationToken.ThrowIfCancellationRequested();

				var index = (RavenJObject)RavenJToken.ReadFrom(_reader);
				if (options.OperateOnTypes.HasFlag(ItemType.Indexes) == false)
					continue;

				var indexName = index.Value<string>("name");
				if (indexName.StartsWith("Temp/"))
					continue;

				var definition = index.Value<RavenJObject>("definition");
				if (definition.Value<bool>("IsCompiled"))
					continue; // can't import compiled indexes

				if (options.OperateOnTypes.HasFlag(ItemType.RemoveAnalyzers))
					definition.Remove("Analyzers");

				var indexDefinition = definition.JsonDeserialization<IndexDefinition>();
				indexDefinition.Name = indexName;

				results.Add(indexDefinition);
			}

			return new CompletedTask<List<IndexDefinition>>(results);
		}

		public Task<LastEtagsInfo> FetchCurrentMaxEtagsAsync()
		{
			return new CompletedTask<LastEtagsInfo>(new LastEtagsInfo
			{
				LastDocsEtag = new Etag(UuidType.Documents, long.MaxValue, long.MaxValue),
				LastDocDeleteEtag = new Etag(UuidType.Documents, long.MaxValue, long.MaxValue)
			});
		}

		public Task<IAsyncEnumerator<RavenJObject>> ReadDocumentsAsync(Etag fromEtag, int pageSize)
		{
			return new CompletedTask<IAsyncEnumerator<RavenJObject>>(new YieldJsonResults<RavenJObject>(fromEtag, _reader, _cancellationToken));
		}

		public Task<RavenJObject> ReadDocumentAsync(string key)
		{
			throw new System.NotImplementedException();
		}

		public Task<DatabaseStatistics> GetStatisticsAsync()
		{
			throw new System.NotImplementedException();
		}

		public bool SupportsGettingStatistics { get; }

		public bool SupportsReadingSingleDocuments { get; }

		public bool SupportsDocumentDeletions { get; }

		public bool SupportsPaging
		{
			get
			{
				return false;
			}
		}

		public bool SupportsRetries
		{
			get
			{
				return false;
			}
		}

		public Task<List<TransformerDefinition>> ReadTransformersAsync(int start, int batchSize, DatabaseSmugglerOptions options)
		{
			throw new System.NotImplementedException();
		}

		public Task<IAsyncEnumerator<string>> ReadDocumentDeletionsAsync(Etag fromEtag, Etag maxEtag)
		{
			throw new System.NotImplementedException();
		}

		public Task<List<KeyValuePair<string, long>>> ReadIdentitiesAsync(DatabaseSmugglerOptions options)
		{
			throw new System.NotImplementedException();
		}

		public Task<SmuggleType> GetNextSmuggleTypeAsync()
		{
			while (_reader.Read() && _reader.TokenType != JsonToken.EndObject)
			{
				_cancellationToken.ThrowIfCancellationRequested();

				if (_reader.TokenType != JsonToken.PropertyName)
					throw new InvalidDataException("PropertyName was expected");

				var currentSection = _reader.Value.ToString();

				if (_reader.Read() == false)
					return new CompletedTask<SmuggleType>(SmuggleType.None);

				if (_reader.TokenType != JsonToken.StartArray)
					throw new InvalidDataException("StartArray was expected");

				switch (currentSection)
				{
					case "Indexes":
						return new CompletedTask<SmuggleType>(SmuggleType.Index);
					case "Docs":
						return new CompletedTask<SmuggleType>(SmuggleType.Document);
					case "Attachments":
						return new CompletedTask<SmuggleType>(SmuggleType.Attachment);
					case "Transformers":
						return new CompletedTask<SmuggleType>(SmuggleType.Transformer);
					case "DocsDeletions":
						return new CompletedTask<SmuggleType>(SmuggleType.DocumentDeletion);
					case "AttachmentsDeletions":
						return new CompletedTask<SmuggleType>(SmuggleType.AttachmentDeletion);
					case "Identities":
						return new CompletedTask<SmuggleType>(SmuggleType.Identity);
					default:
						throw new NotSupportedException("Not supported section: " + currentSection);
				}
			}

			return new CompletedTask<SmuggleType>(SmuggleType.None);
		}

		public void Dispose()
		{
			_streamReader?.Dispose();

			_sizeStream?.Dispose();
		}
	}

	public class YieldJsonResults<T> : IAsyncEnumerator<RavenJObject>
	{
		private readonly Etag _fromEtag;

		private readonly JsonTextReader _reader;

		private readonly CancellationToken _cancellationToken;

		public YieldJsonResults(Etag fromEtag, JsonTextReader reader, CancellationToken cancellationToken)
		{
			_fromEtag = fromEtag;
			_reader = reader;
			_cancellationToken = cancellationToken;
		}

		public void Dispose()
		{
			Current = null;
		}

		public Task<bool> MoveNextAsync()
		{
			Current = null;

			while (_reader.Read() && _reader.TokenType != JsonToken.EndArray)
			{
				_cancellationToken.ThrowIfCancellationRequested();

				var document = (RavenJObject)RavenJToken.ReadFrom(_reader);
				var etag = Etag.Parse(document.Value<RavenJObject>("@metadata").Value<string>("@etag"));
				if (EtagUtil.IsGreaterThan(_fromEtag, etag))
					continue;

				Current = document;
				return new CompletedTask<bool>(true);
			}

			return new CompletedTask<bool>(false);
		}

		public RavenJObject Current { get; private set; }
	}
}