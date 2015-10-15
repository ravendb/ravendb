using System;
using System.IO;
using Voron;
using Voron.Impl;
using Voron.Trees;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesLogStorage
	{
		private readonly byte[] keyBuffer = new byte[sizeof(long)];
		private readonly Tree openLog;
		private long lastEtag;

		public TimeSeriesLogStorage(Transaction tx)
		{
			openLog = tx.ReadTree(TimeSeriesStorage.TreeNames.OpenLog);
			lastEtag = GetLastEtag();
			// tx.ReadTree(TimeSeriesStorage.TreeNames.CompressedLog);
		}

		public long GetLastEtag()
		{
			var lastKey = openLog.LastKeyOrDefault();
			var etag = lastKey?.CreateReader().ReadBigEndianInt64() ?? 0;
			return etag;
		}

		private void WriteToLog(Action<BinaryWriter> writeAction)
		{
			using (var ms = new MemoryStream())
			using (var writer = new BinaryWriter(ms))
			{
				writeAction(writer);
                writer.Flush();
				ms.Position = 0;
				openLog.Add(GetNextEtagKey(), ms);
			}
		}

		private Slice GetNextEtagKey()
		{
			var nextEtag = ++lastEtag;
			var keyWriter = new SliceWriter(keyBuffer);
			keyWriter.WriteBigEndian(nextEtag);
			return keyWriter.CreateSlice();
		}

		private enum LogTypes : byte
		{
			Append = 1,
			DeleteKey = 21,
			DeletePoint = 22,
			DeleteRange = 23,
		}

		public void Append(string type, string key, long time, params double[] values)
		{
			WriteToLog(writer =>
			{
				writer.Write((byte)LogTypes.Append);
				writer.Write(type);
				writer.Write(key);
				writer.Write(time);
				writer.Write(values.Length);
				foreach (var value in values)
				{
					writer.Write(value);
				}
			});
		}

		public void DeleteKey(string type, string key)
		{
			WriteToLog(writer =>
			{
				writer.Write((byte)LogTypes.DeleteKey);
				writer.Write(type);
				writer.Write(key);
			});
		}

		public void DeletePoint(string type, string key, long time)
		{
			WriteToLog(writer =>
			{
				writer.Write((byte)LogTypes.DeletePoint);
				writer.Write(type);
				writer.Write(key);
				writer.Write(time);
			});
		}

		public void DeleteRange(string type, string key, long start, long end)
		{
			WriteToLog(writer =>
			{
				writer.Write((byte)LogTypes.DeleteRange);
				writer.Write(type);
				writer.Write(key);
				writer.Write(start);
				writer.Write(end);
			});
		}
	}
}