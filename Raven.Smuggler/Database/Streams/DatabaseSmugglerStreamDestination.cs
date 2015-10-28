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

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Database.Streams
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

		public virtual Task InitializeAsync(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, CancellationToken cancellationToken)
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
            return new DatabaseSmugglerStreamDocumentDeletionActions(_writer);
        }

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			return new DatabaseSmugglerStreamIdentityActions(_writer);
		}

		public virtual Task<DatabaseSmugglerOperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public virtual Task SaveOperationStateAsync(DatabaseSmugglerOptions options, DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

		public Task WaitForIndexingAsOfLastWriteAsync(CancellationToken cancellationToken)
		{
			throw new NotSupportedException();
		}

	    public virtual Task AfterExecuteAsync(DatabaseSmugglerOperationState state)
	    {
            return new CompletedTask();
        }

	    public virtual void OnException(SmugglerException exception)
	    {
	    }

	    public void Dispose()
		{
			_writer?.WriteEndObject();
            _writer?.Flush();

		    _streamWriter?.Flush();
			_streamWriter?.Dispose();

            _gZipStream?.Dispose();

		    if (_leaveOpen == false)
		    {
                _stream?.Flush();
                _stream?.Dispose();
            }
		}
	}
}