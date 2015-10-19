// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.IO.Compression;

using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
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

		public virtual void Initialize(DatabaseSmugglerOptions options)
		{
			_gZipStream = new GZipStream(_stream, CompressionMode.Compress, leaveOpen: true);
			_streamWriter = new StreamWriter(_gZipStream);
			_writer = new JsonTextWriter(_streamWriter)
			{
				Formatting = Formatting.Indented
			};

			_writer.WriteStartObject();
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
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			return new DatabaseSmugglerStreamIdentityActions(_writer);
		}

		public virtual OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state)
		{
			return state;
		}

		public void Dispose()
		{
			_writer.WriteEndObject();

			_streamWriter?.Dispose();

			_gZipStream?.Dispose();

			if (_leaveOpen == false)
				_stream.Dispose();
		}
	}
}