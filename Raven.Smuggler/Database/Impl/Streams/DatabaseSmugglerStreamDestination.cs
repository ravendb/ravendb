// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerStreamDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.IO.Compression;

using Raven.Imports.Newtonsoft.Json;

namespace Raven.Smuggler.Database.Impl.Streams
{
	public class DatabaseSmugglerStreamDestination : IDatabaseSmugglerDestination
	{
		private readonly Stream _stream;

		private GZipStream _gZipStream;

		private StreamWriter _streamWriter;

		private JsonTextWriter _writer;

		public DatabaseSmugglerStreamDestination(Stream stream)
		{
			_stream = stream;
		}

		public void Initialize()
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
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerDocumentDeletionActions DocumentDeletionActions()
		{
			throw new System.NotImplementedException();
		}

		public IDatabaseSmugglerIdentityActions IdentityActions()
		{
			throw new System.NotImplementedException();
		}

		public void Dispose()
		{
			_writer.WriteEndObject();

			_streamWriter?.Dispose();

			_gZipStream?.Dispose();
		}
	}
}