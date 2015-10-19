using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Abstractions.Logging;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Raven.Database.TimeSeries
{
	public class TimeSeriesLogStorage
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

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

		public IEnumerable<ReplicationLogItem> GetLogsSinceEtag(long etag)
		{
			if (etag > lastEtag)
				yield break;

			using (var it = openLog.Iterate())
			{
				var key = new Slice(EndianBitConverter.Big.GetBytes(etag));
				if (it.Seek(key) == false)
					yield break;

				var valueReader = it.CreateReaderForCurrent();
				var buffer = new byte[valueReader.Length];
				valueReader.Read(buffer, 0, buffer.Length);
				yield return new ReplicationLogItem
				{
					Etag = it.CurrentKey.CreateReader().ReadBigEndianInt64(),
					BinaryData = buffer,
				};
			}
		}

		private void WriteToLog(LogTypes logType, Action<BinaryWriter> writeAction)
		{
			using (var ms = new MemoryStream())
			using (var writer = new BinaryWriter(ms, Encoding.UTF8))
			{
				writer.Write((byte) logType);
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

			CreateType = 31,
			DeleteType = 33,
		}

		public void Append(string type, string key, long time, params double[] values)
		{
			WriteToLog(LogTypes.Append, writer =>
            {
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
			WriteToLog(LogTypes.DeleteKey, writer =>
            {
				writer.Write(type);
				writer.Write(key);
			});
		}

		public void DeletePoint(string type, string key, long time)
		{
			WriteToLog(LogTypes.DeletePoint, writer =>
			{
				writer.Write(type);
				writer.Write(key);
				writer.Write(time);
			});
		}

		public void DeleteRange(string type, string key, long start, long end)
		{
			WriteToLog(LogTypes.DeleteRange, writer =>
            {
				writer.Write(type);
				writer.Write(key);
				writer.Write(start);
				writer.Write(end);
			});
		}

		public void CreateType(string type, string[] fields)
		{
			WriteToLog(LogTypes.CreateType, writer =>
            {
				writer.Write(type);
				writer.Write(fields.Length);
				foreach (var field in fields)
				{
					writer.Write(field);
				}
			});
		}

		public void DeleteType(string type)
		{
			WriteToLog(LogTypes.DeleteType, writer =>
            {
				writer.Write(type);
			});
		}

		public void PostReplicationLogItem(ReplicationLogItem logItem, TimeSeriesStorage.Writer writer)
		{
			using (var stream = new MemoryStream(logItem.BinaryData))
			using (var reader = new BinaryReader(stream, Encoding.UTF8))
			{
				var readByte = (LogTypes)reader.ReadByte();
				switch (readByte)
				{
					case LogTypes.Append:
						throw new NotImplementedException();
						break;
					case LogTypes.DeleteKey:
						throw new NotImplementedException();
						break;
					case LogTypes.DeletePoint:
						throw new NotImplementedException();
						break;
					case LogTypes.DeleteRange:
						throw new NotImplementedException();
						break;
					case LogTypes.CreateType:
						ReplicationCreateType(reader, writer);
						break;
					case LogTypes.DeleteType:
						ReplicationDeleteType(reader, writer);
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
			}
		}

		public string ReadType(BinaryReader reader)
		{
			var type = reader.ReadString();
			if (string.IsNullOrWhiteSpace(type))
			{
				log.Warn("Time Series Replication post fail: type cannot be null or empty");
				throw new InvalidOperationException("Time Series Replication post fail: type cannot be null or empty");
			}
			return type;
		}

		private void ReplicationDeleteType(BinaryReader reader, TimeSeriesStorage.Writer writer)
		{
			var type = ReadType(reader);
			writer.DoDeleteType(type);
		}

		private void ReplicationCreateType(BinaryReader reader, TimeSeriesStorage.Writer writer)
		{
			var type = reader.ReadString();

			var fieldsCount = reader.ReadInt32();
			var fields = new string[fieldsCount];
			for (int i = 0; i < fieldsCount; i++)
			{
				fields[i] = reader.ReadString();
			}
			
			writer.DoCreateType(type, fields);
		}
	}
}