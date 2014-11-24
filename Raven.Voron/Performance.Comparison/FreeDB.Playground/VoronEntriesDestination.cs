using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace FreeDB.Playground
{
	public class VoronEntriesDestination : EntryDestination
	{
		private readonly StorageEnvironment _storageEnvironment;
		private WriteBatch _currentBatch;
		private int counter = 1;

		public VoronEntriesDestination()
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

		public override int Accept(string d)
		{
			var disk = JObject.Parse(d);

			var ms = new MemoryStream();
			var writer = new StreamWriter(ms);
			writer.Write(d);
			writer.Flush();
			ms.Position = 0;
			var key = new Slice(EndianBitConverter.Big.GetBytes(counter++));
			_currentBatch.Add(key, ms, "albums");
			int count = 1;

			foreach (var diskId in disk.Value<JArray>("DiskIds"))
			{
				count++;
				_currentBatch.MultiAdd(diskId.Value<string>(), key, "ix_diskids");
			}

			var artist = disk.Value<string>("Artist");
			if (artist != null)
			{
				count++; 
				_currentBatch.MultiAdd(artist.ToLower(), key, "ix_artists");
			}
			var title = disk.Value<string>("Title");
			if (title != null)
			{
				count++; 
				_currentBatch.MultiAdd(title.ToLower(), key, "ix_titles");
			}

			if (counter % 500 == 0)
			{
				_storageEnvironment.Writer.Write(_currentBatch);
				_currentBatch = new WriteBatch();
			}
			return count;
		}

		public override void Done()
		{
			_storageEnvironment.Writer.Write(_currentBatch);
			_storageEnvironment.Dispose();
		}
	}
}