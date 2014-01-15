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
                _storageEnvironment.CreateTree(tx, "data");
                var read = tx.State.Root.Read(tx, _lastKey);

                _last = read != null ? read.Reader.ReadInt64() : 1;

                tx.Commit();
            }
        }

        public void Add(string id, DateTime dateTime, double value)
        {
            AddRange(id, new []{new KeyValuePair<DateTime, double>(dateTime, value), });
        }

        public void AddRange(string id, IEnumerable<KeyValuePair<DateTime, double>> values)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var data = _storageEnvironment.State.GetTree(tx, "data");
                var buffer = new byte[16];
                var key = new Slice(buffer);
                var ms = new MemoryStream();
                var bw = new BinaryWriter(ms);
                foreach (var kvp in values)
                {
                    var date = kvp.Key;
                    EndianBitConverter.Big.CopyBytes(date.ToBinary(), buffer, 0);
                    EndianBitConverter.Big.CopyBytes(_last++, buffer, 8);
                    ms.SetLength(0);
                    bw.Write(kvp.Value);
                    ms.Position = 0;

                    data.Add(tx, key, ms);
                }

                tx.State.Root.Add(tx, _lastKey, new MemoryStream(BitConverter.GetBytes(_last)));
                tx.Commit();
            }
        }

        public IEnumerable<double> ScanRange(string id, DateTime start, DateTime end)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
            {
                var data = _storageEnvironment.State.GetTree(tx, "data");
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
                    var buffer = new byte[sizeof (double)];
                    do
                    {
                        var reader = it.CreateReaderForCurrent();
                        var n = reader.Read(buffer,0, sizeof(double));
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