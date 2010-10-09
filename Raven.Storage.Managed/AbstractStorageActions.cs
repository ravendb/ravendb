using System;
using System.IO;
using Raven.Storage.Managed.Data;

namespace Raven.Storage.Managed
{
	public class AbstractStorageActions
	{
		private StorageMutator mutator;

		public Stream Reader { get; set; }
		private Stream writer;
		private BinaryReaderWith7BitEncoding binaryReader;
		private BinaryWriterWith7BitEncoding binaryWriter;

		public Stream Writer
		{
			get
			{
				if (writer == null)
					throw new InvalidOperationException("Cannot perform write operation in read only mode");
				
				return writer;
			}
			set { writer = value; }
		}

		public BinaryWriterWith7BitEncoding BinaryWriter
		{
			get
			{
				if(binaryWriter == null)
					binaryWriter = new BinaryWriterWith7BitEncoding(Writer);
				return binaryWriter;
			}
		}

		public BinaryReaderWith7BitEncoding BinaryReader
		{
			get
			{
				if(binaryReader == null)
					binaryReader = new BinaryReaderWith7BitEncoding(Reader);
				return binaryReader;
			}
		}

		public StorageMutator Mutator
		{
			get
			{
				if(mutator==null)
					throw new InvalidOperationException("Cannot perform write operation in read only mode");
				return mutator;
			}
			set { mutator = value; }
		}

		public StorageReader Viewer
		{
			get; set;
		}
	}
}