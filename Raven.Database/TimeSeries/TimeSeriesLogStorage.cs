using System;
using System.Collections.Generic;
using System.IO;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util.Conversion;

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
            var etag = (lastKey!=null)?lastKey.CreateReader().ReadBigEndianInt64() : 0;
            return etag;
        }

        public IEnumerable<ReplicationLogItem> GetLogsSinceEtag(long etag)
        {
            if (etag == lastEtag)
                yield break;

            using (var it = openLog.Iterate())
            {
                var key = new Slice(EndianBitConverter.Big.GetBytes(etag));
                if (it.Seek(key) == false)
                    yield break;

                yield return new ReplicationLogItem
                {
                    Etag = it.CurrentKey.CreateReader().ReadBigEndianInt64(),
                    // TODO: BinaryData = it.CreateReaderForCurrent().,
                };
            }
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
