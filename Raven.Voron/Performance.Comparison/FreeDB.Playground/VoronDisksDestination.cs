using System.IO;
using Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;
using XmcdParser;

namespace FreeDB.Playground
{
	public class VoronDisksDestination : DisksDestination
	{
		private readonly StorageEnvironment _storageEnvironment;
		private WriteBatch _currentBatch;
		private readonly JsonSerializer _serializer = new JsonSerializer();
		private int counter = 1;

		public VoronDisksDestination()
		{
			_storageEnvironment = new StorageEnvironment(StorageEnvironmentOptions.ForPath("FreeDB"));
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				_storageEnvironment.CreateTree(tx, "albums");
				_storageEnvironment.CreateTree(tx, "ix_diskids");
				_storageEnvironment.CreateTree(tx, "ix_artists");
				_storageEnvironment.CreateTree(tx, "ix_titles");
				tx.Commit();
			}
			_currentBatch = new WriteBatch();
		}

		public override void Accept(Disk d)
		{
			var ms = new MemoryStream();
			_serializer.Serialize(new JsonTextWriter(new StreamWriter(ms)), d);
			ms.Position = 0;
			var key = new Slice(EndianBitConverter.Big.GetBytes(counter++));
			_currentBatch.Add(key, ms, "albums");

			foreach (var diskId in d.DiskIds)
			{
				_currentBatch.MultiAdd(diskId, key, "ix_diskids");
			}

			if(d.Artist != null)
				_currentBatch.MultiAdd(d.Artist.ToLower(), key, "ix_artists");
			if (d.Title != null)
				_currentBatch.MultiAdd(d.Title.ToLower(), key, "ix_titles");

			if (counter%1000 == 0)
			{
				_storageEnvironment.Writer.Write(_currentBatch);
				_currentBatch = new WriteBatch();
			}

		}

		public override void Done()
		{
			_storageEnvironment.Writer.Write(_currentBatch);
		}
	}
}