using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Voron;
using XmcdParser;

namespace FreeDB.Playground
{
	public class FreeDbQueries : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;
		private readonly JsonSerializer _serializer = new JsonSerializer();
		public FreeDbQueries(string path)
		{
			_storageEnvironment = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path));
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				_storageEnvironment.CreateTree(tx, "albums");
				_storageEnvironment.CreateTree(tx, "ix_diskids");
				_storageEnvironment.CreateTree(tx, "ix_artists");
				_storageEnvironment.CreateTree(tx, "ix_titles");
				tx.Commit();
			}
		}

		public IEnumerable<Disk> Page()
		{
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var albums = tx.GetTree("albums");
				using (var it = albums.Iterate(tx))
				{
					it.Seek(Slice.BeforeAllKeys);
					var stringValue = it.CreateReaderForCurrent().ToStringValue();
				}
				tx.Commit();
			}
			return null;
		}

		public IEnumerable<Disk> FindByDiskId(string diskId)
		{
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var dix = tx.GetTree("ix_diskids");
				var albums = tx.GetTree("albums");

				using (var it = dix.MultiRead(tx, diskId))
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						var readResult = albums.Read(tx, it.CurrentKey);
						using (var stream = readResult.Reader.AsStream())
						{
							yield return _serializer.Deserialize<Disk>(new JsonTextReader(new StreamReader(stream)));
						}
					} while (it.MoveNext());
				}

				tx.Commit();
			}
		}

		public void Dispose()
		{
			_storageEnvironment.Dispose();
		}
	}
}