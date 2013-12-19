using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Trees;
using XmcdParser;

namespace FreeDB.Playground
{
	public class FreeDbQueries : IDisposable
	{
		private readonly JsonSerializer _serializer = new JsonSerializer();
		private readonly StorageEnvironment _storageEnvironment;

		public FreeDbQueries(string path)
		{
			_storageEnvironment = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path));
			using (Transaction tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				_storageEnvironment.CreateTree(tx, "albums");
				_storageEnvironment.CreateTree(tx, "ix_diskids");
				_storageEnvironment.CreateTree(tx, "ix_artists");
				_storageEnvironment.CreateTree(tx, "ix_titles");
				tx.Commit();
			}
		}

		public void Dispose()
		{
			_storageEnvironment.Dispose();
		}

		public IEnumerable<Disk> FindByArtist(string prefix)
		{
			return FindByMultiValueIterator(prefix, "ix_artists");
		}

		public IEnumerable<Disk> FindByAlbumTitle(string prefix)
		{
			return FindByMultiValueIterator(prefix, "ix_titles");
		}

		private IEnumerable<Disk> FindByMultiValueIterator(string prefix, string treeIndexName)
		{
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var dix = tx.GetTree(treeIndexName);
				var albums = tx.GetTree("albums");

				using (var multiValueIterator = dix.Iterate(tx))
				{
					multiValueIterator.RequiredPrefix = prefix.ToLower();
					if (multiValueIterator.Seek(multiValueIterator.RequiredPrefix) == false)
						yield break;
					do
					{
						using (var albumsIterator = multiValueIterator.CreateMutliValueIterator())
						{
							if (albumsIterator.Seek(Slice.BeforeAllKeys) == false)
								continue;
							do
							{
								var readResult = albums.Read(tx, albumsIterator.CurrentKey);
								using (var stream = readResult.Reader.AsStream())
								{
									yield return _serializer.Deserialize<Disk>(new JsonTextReader(new StreamReader(stream)));
								}
							} while (albumsIterator.MoveNext());
						}
					} while (multiValueIterator.MoveNext());
				}

				tx.Commit();
			}
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
	}
}