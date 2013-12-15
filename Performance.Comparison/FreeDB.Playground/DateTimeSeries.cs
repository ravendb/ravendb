using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Util.Conversion;

namespace FreeDB.Playground
{
public class DateTimeSeries : IDisposable
{
	private readonly JsonSerializer _serializer = new JsonSerializer();
	private readonly StorageEnvironment _storageEnvironment;
	private long _last;
	private readonly Slice _lastKey;

	public DateTimeSeries(string path)
	{
		_lastKey = "last-key";
		_storageEnvironment = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path));
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
		{
			_storageEnvironment.CreateTree(tx, "data");
			var read = tx.State.Root.Read(tx, _lastKey);

			_last = read != null ? read.Reader.ReadInt64() : 1;

			tx.Commit();
		}
	}

	public void AddRange<T>(IEnumerable<KeyValuePair<DateTime, T>> values)
	{
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
		{
			var data = tx.GetTree("data");
			var buffer = new byte[16];
			var key = new Slice(buffer);
			var ms = new MemoryStream();
			foreach (var kvp in values)
			{
				var date = kvp.Key;
				EndianBitConverter.Big.CopyBytes(date.ToBinary(), buffer, 0);
				EndianBitConverter.Big.CopyBytes(_last++, buffer, 8);
				ms.SetLength(0);
				_serializer.Serialize(new StreamWriter(ms), kvp.Value);
				ms.Position = 0;

				data.Add(tx, key, ms);
			}

			tx.State.Root.Add(tx, _lastKey, new MemoryStream(BitConverter.GetBytes(_last)));
			tx.Commit();
		}
	}

	public IEnumerable<T> ScanRange<T>(DateTime start, DateTime end)
	{
		using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
		{
			var data = tx.GetTree("data");
			var startBuffer = new byte[16];
			EndianBitConverter.Big.CopyBytes(start.ToBinary(), startBuffer, 0);
			var startKey = new Slice(startBuffer);

			using (var it = data.Iterate(tx))
			{
				var endBuffer = new byte[16];
				EndianBitConverter.Big.CopyBytes(end.ToBinary(), endBuffer, 0);
				EndianBitConverter.Big.CopyBytes(long.MaxValue, endBuffer, 8);

				it.MaxKey = new Slice(endBuffer);
				if (it.Seek(startKey) == false)
					yield break;
				do
				{
					var reader = it.CreateReaderForCurrent();
					using (var stream = reader.AsStream())
					{
						yield return _serializer.Deserialize<T>(new JsonTextReader(new StreamReader(stream)));
					}
				} while (it.MoveNext());
			}
		}
			
	}

	public void Dispose()
	{
		_storageEnvironment.Dispose();
	}
}
}