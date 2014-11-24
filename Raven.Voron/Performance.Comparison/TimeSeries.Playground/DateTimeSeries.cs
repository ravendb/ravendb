using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
        private readonly Dictionary<string, List<Entry>> _buffer = new Dictionary<string, List<Entry>>();

        public struct Entry
        {
            public readonly DateTime Timestamp;
            public readonly double Value;

            public Entry(DateTime dt, double value)
            {
                Timestamp = dt;
                Value = value;
            }
        }

        public void Add(string id, DateTime dateTime, double value)
        {
            List<Entry> list;
            if (_buffer.TryGetValue(id, out list) == false)
                _buffer[id] = list = new List<Entry>();

            list.Add(new Entry(dateTime, value));
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
                        var date = item.Timestamp;
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

        private class PendingEnumerator
        {
            public IEnumerator<Entry> Enumerator;
            public int Index;
        }

        private class PendingEnumerators
        {
            private readonly SortedDictionary<DateTime, List<PendingEnumerator>> _values =
                new SortedDictionary<DateTime, List<PendingEnumerator>>();

            public void Enqueue(PendingEnumerator entry)
            {
                List<PendingEnumerator> list;
                var dateTime = entry.Enumerator.Current.Timestamp;
                if (_values.TryGetValue(dateTime, out list) == false)
                {
                    _values.Add(dateTime, list = new List<PendingEnumerator>());
                }
                list.Add(entry);
            }

            public bool IsEmpty { get { return _values.Count == 0; } }

            public List<PendingEnumerator> Dequeue()
            {
                if (_values.Count == 0)
                    return new List<PendingEnumerator>();

                var kvp = _values.First();
                _values.Remove(kvp.Key);
                return kvp.Value;
            }
        }

        public class RangeEntry
        {
            public DateTime Timestamp;
            public double?[] Values;
        }

        public IEnumerable<RangeEntry> ScanRanges(DateTime start, DateTime end, string[] ids)
        {
            if (ids == null || ids.Length == 0)
                yield break;

            var pending = new PendingEnumerators();
            for (int i = 0; i < ids.Length; i++)
            {
                var enumerator = ScanRange(start, end, ids[i]).GetEnumerator();
                if(enumerator.MoveNext() == false)
                    continue;
                pending.Enqueue(new PendingEnumerator
                {
                    Enumerator = enumerator,
                    Index = i
                });
            }

            var result = new RangeEntry
            {
                Values = new double?[ids.Length]
            };
            while (pending.IsEmpty == false)
            {
                Array.Clear(result.Values,0,result.Values.Length);
                var entries = pending.Dequeue();
                if (entries.Count == 0)
                    break;
                foreach (var entry in entries)
                {
                    var current = entry.Enumerator.Current;
                    result.Timestamp = current.Timestamp;
                    result.Values[entry.Index] = current.Value;
                    if(entry.Enumerator.MoveNext())
                        pending.Enqueue(entry);
                }
                yield return result;
            }
        }

        public IEnumerable<Entry> ScanRange(DateTime start, DateTime end, string id)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
            {

                var tree = _storageEnvironment.State.GetTree(tx, "channel:" + id);
                var startBuffer = new byte[16];
                EndianBitConverter.Big.CopyBytes(start.Ticks, startBuffer, 0);
                var startKey = new Slice(startBuffer);

                using (var it = tree.Iterate(tx))
                {
                    var endBuffer = new byte[16];
                    EndianBitConverter.Big.CopyBytes(end.Ticks, endBuffer, 0);
                    EndianBitConverter.Big.CopyBytes(long.MaxValue, endBuffer, 8);

                    it.MaxKey = new Slice(endBuffer);
                    if (it.Seek(startKey) == false)
                        yield break;
                    var valueBuffer = new byte[sizeof(double)];
                    var keyBuffer = new byte[16];
                    do
                    {
                        var reader = it.CreateReaderForCurrent();
                        var n = reader.Read(valueBuffer, 0, sizeof(double));
                        Debug.Assert(n == sizeof(double));
                        it.CurrentKey.CopyTo(keyBuffer);
                        var dt = new DateTime(EndianBitConverter.Big.ToInt64(keyBuffer, 0));
                        var value = BitConverter.ToDouble(valueBuffer, 0);
                        yield return new Entry(dt, value);
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
