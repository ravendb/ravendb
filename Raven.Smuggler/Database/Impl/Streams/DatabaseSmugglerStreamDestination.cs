// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamDestination : IDatabaseSmugglerDestination
	{
		protected Stream _stream;

		private readonly bool _leaveOpen;

		private GZipStream _gZipStream;

		private StreamWriter _streamWriter;

		private JsonTextWriter _writer;

		public DatabaseSmugglerStreamDestination(Stream stream, bool leaveOpen = true)
		{
			_stream = stream;
			_leaveOpen = leaveOpen;
		}

		public virtual bool SupportsOperationState => false;

		public bool SupportsWaitingForIndexing => false;

		public virtual Task InitializeAsync(DatabaseSmugglerOptions options, Report report, CancellationToken cancellationToken)
		{
			_gZipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
			_streamWriter = new StreamWriter(_gZipStream);
			_writer = new JsonTextWriter(_streamWriter)
			{
				Formatting = Formatting.Indented
			};

			_writer.WriteStartObject();
			return new CompletedTask();
		}

		public IDatabaseSmugglerIndexActions IndexActions()
		{
			return new DatabaseSmugglerStreamIndexActions(_writer);
		}

		public IDatabaseSmugglerDocumentActions DocumentActions()
		{
			return new DatabaseSmugglerStreamDocumentActions(_writer);
		}

		public IDatabaseSmugglerTransformerActions TransformerActions()
		{
			return new DatabaseSmugglerStreamTransformerActions(_writer);
		}

		public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
		{
			throw new NotSupportedException();
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			return new DatabaseSmugglerStreamIdentityActions(_writer);
		}

		public virtual Task<OperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public virtual Task SaveOperationStateAsync(DatabaseSmugglerOptions options, OperationState state, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public void Dispose()
		{
			_writer?.WriteEndObject();

			_streamWriter?.Dispose();

			_gZipStream?.Dispose();

			if (_leaveOpen == false)
				_stream?.Dispose();
		}
	}
}