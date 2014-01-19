using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Newtonsoft.Json;
using Voron;
using Voron.Util.Conversion;

namespace TimeSeries.Playground
{
	public class DateTimeSeries : IDisposable
	{
		private readonly StorageEnvironment _storageEnvironment;
		private long _last;
		private readonly Slice _lastKey;

		public DateTimeSeries(string path)
		{
			_lastKey = "last-key";
			_storageEnvironment = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path));
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				var read = tx.State.Root.Read(tx, _lastKey);

				_last = read != null ? read.Reader.ReadInt64() : 1;

				tx.Commit();
			}
		}

		private int _bufferSize;
		private readonly Dictionary<string, List<KeyValuePair<DateTime, double>>> _buffer = new Dictionary<string, List<KeyValuePair<DateTime, double>>>();

		public void Add(string id, DateTime dateTime, double value)
		{
			List<KeyValuePair<DateTime, double>> list;
			if (_buffer.TryGetValue(id, out list) == false)
				_buffer[id] = list = new List<KeyValuePair<DateTime, double>>();

			list.Add(new KeyValuePair<DateTime, double>(dateTime, value));
			_bufferSize++;
			if (_bufferSize > 1000)
				Flush();
		}

		public IEnumerable<string> ScanIds()
		{
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				using (var it = tx.State.Root.Iterate(tx))
				{
					var prefix = "channel:";
					it.RequiredPrefix = prefix;
					if (it.Seek(it.RequiredPrefix) == false)
						yield break;
					do
					{
						var key = it.CurrentKey.ToString();
						yield return key.Substring(prefix.Length);
					} while (it.MoveNext());
				}
			}
		}

		public void Flush()
		{
			_bufferSize = 0;

			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				foreach (var kvp in _buffer)
				{
					var data = _storageEnvironment.CreateTree(tx, "channel:" + kvp.Key);
					var buffer = new byte[16];
					var key = new Slice(buffer);
					var ms = new MemoryStream();
					var bw = new BinaryWriter(ms);
					foreach (var item in kvp.Value)
					{
						var date = item.Key;
						EndianBitConverter.Big.CopyBytes(date.Ticks, buffer, 0);
						EndianBitConverter.Big.CopyBytes(_last++, buffer, 8);
						ms.SetLength(0);
						bw.Write(item.Value);
						ms.Position = 0;

						data.Add(tx, key, ms);
					}
				}

				tx.State.Root.Add(tx, _lastKey, new MemoryStream(BitConverter.GetBytes(_last)));
				tx.Commit();
			}
			_buffer.Clear();
		}

		public IEnumerable<double> ScanRange(string id, DateTime start, DateTime end)
		{
			using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
			{
				var data = _storageEnvironment.State.GetTree(tx, "channel:" + id);
				var startBuffer = new byte[16];
				EndianBitConverter.Big.CopyBytes(start.Ticks, startBuffer, 0);
				var startKey = new Slice(startBuffer);

				using (var it = data.Iterate(tx))
				{
					var endBuffer = new byte[16];
					EndianBitConverter.Big.CopyBytes(end.Ticks, endBuffer, 0);
					EndianBitConverter.Big.CopyBytes(long.MaxValue, endBuffer, 8);

					it.MaxKey = new Slice(endBuffer);
					if (it.Seek(startKey) == false)
						yield break;
					var buffer = new byte[sizeof(double)];
					do
					{
						var reader = it.CreateReaderForCurrent();
						var n = reader.Read(buffer, 0, sizeof(double));
						Debug.Assert(n == sizeof(double));
						yield return BitConverter.ToDouble(buffer, 0);
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