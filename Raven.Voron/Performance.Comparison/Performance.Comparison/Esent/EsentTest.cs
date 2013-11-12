namespace Performance.Comparison.Esent
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;

    using Microsoft.Isam.Esent.Interop;

    public class EsentTest : StoragePerformanceTestBase
    {
        private readonly string _path;

        private readonly Configurator _configurator;

        private JET_INSTANCE _instance;

        private JET_SESID _sesid;

        private JET_TABLEID _tableid;

        private JET_COLUMNDEF _primaryColumn;

        private JET_COLUMNDEF _secondaryColumn;

        private JET_COLUMNID _primaryColumnId;

        private JET_COLUMNID _secondaryColumnId;

        private JET_DBID _dbid;

        public override string StorageName
        {
            get
            {
                return "Esent";
            }
        }

        public EsentTest(string path, byte[] buffer)
            : base(buffer)
        {
            _path = Path.Combine(path, "esent");
            _configurator = new Configurator();
        }

        ~EsentTest()
        {
            if (Directory.Exists(_path))
                Directory.Delete(_path, true);
        }

        public void CreateInstance(bool delete = true)
        {
            if (delete && Directory.Exists(_path))
                Directory.Delete(_path, true);

            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            Api.JetCreateInstance(out _instance, Guid.NewGuid().ToString());

            _configurator.ConfigureInstance(_instance, _path);

            Api.JetInit(ref _instance);
            Api.JetBeginSession(_instance, out _sesid, null, null);

            if (delete)
            {
                Api.JetCreateDatabase(_sesid, "edbtest.db", null, out _dbid, CreateDatabaseGrbit.OverwriteExisting);
                CreateSchema();
            }
            else
            {
                Api.JetAttachDatabase2(_sesid, "edbtest.db", 0, AttachDatabaseGrbit.None);
                Api.JetOpenDatabase(_sesid, "edbtest.db", null, out _dbid, OpenDatabaseGrbit.None);
                OpenSchema();
            }
        }

        private void OpenSchema()
        {
            Api.JetOpenTable(_sesid, _dbid, "table", null, 0, OpenTableGrbit.ReadOnly, out _tableid);
            _primaryColumnId = Api.GetTableColumnid(_sesid, _tableid, "key");
            _secondaryColumnId = Api.GetTableColumnid(_sesid, _tableid, "data");
        }

        private void CreateSchema()
        {
            using (var tx = new Transaction(_sesid))
            {
                Api.JetCreateTable(_sesid, _dbid, "table", 0, 100, out _tableid);
                _primaryColumn = new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Long
                };

                _secondaryColumn = new JET_COLUMNDEF
                {
                    coltyp = JET_coltyp.Binary
                };

                Api.JetAddColumn(_sesid, _tableid, "key", _primaryColumn, null, 0, out _primaryColumnId);
                Api.JetAddColumn(_sesid, _tableid, "data", _secondaryColumn, null, 0, out _secondaryColumnId);

                var index = new JET_INDEXCREATE
                {
                    szKey = "+key\0\0",
                    szIndexName = "by_key",
                    grbit = CreateIndexGrbit.IndexPrimary,
                    ulDensity = 90
                };

                Api.JetCreateIndex(_sesid, _tableid, index.szIndexName, index.grbit, index.szKey, index.szKey.Length, index.ulDensity);

                tx.Commit(CommitTransactionGrbit.None);
            }
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data)
        {
            return Write(string.Format("[Esent] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data)
        {
            return Write(string.Format("[Esent] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public override PerformanceRecord ReadSequential()
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[Esent] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds)
        {
            return Read(string.Format("[Esent] random read ({0} items)", Constants.ReadItems), randomIds);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions)
        {
            try
            {
                byte[] valueToWrite = null;
                var records = new List<PerformanceRecord>();

                CreateInstance();

                var sw = new Stopwatch();

                var enumerator = data.GetEnumerator();

                for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                {
                    sw.Restart();

                    using (var tx = new Transaction(_sesid))
                    {
                        for (var i = 0; i < itemsPerTransaction; i++)
                        {
                            enumerator.MoveNext();

                            valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                            Api.JetPrepareUpdate(_sesid, _tableid, JET_prep.Insert);
                            Api.SetColumn(_sesid, _tableid, _primaryColumnId, enumerator.Current.Id);
                            Api.SetColumn(_sesid, _tableid, _secondaryColumnId, valueToWrite);
                            Api.JetUpdate(_sesid, _tableid);
                        }

                        tx.Commit(CommitTransactionGrbit.None);
                    }

                    sw.Stop();

                    records.Add(new PerformanceRecord
                    {
                        Operation = operation,
                        Time = DateTime.Now,
                        Duration = sw.ElapsedMilliseconds,
                        ProcessedItems = itemsPerTransaction,
                        Memory = GetMemory()
                    });
                }

                sw.Stop();

                return records;
            }
            finally
            {
                Close();
            }
        }

        private PerformanceRecord Read(string operation, IEnumerable<int> ids)
        {
            try
            {
                CreateInstance(delete: false);

                var sw = Stopwatch.StartNew();

                var processed = 0;

                Api.JetSetCurrentIndex(_sesid, _tableid, "by_key");

                foreach (var id in ids)
                {
                    Api.MakeKey(_sesid, _tableid, id, MakeKeyGrbit.NewKey);
                    Api.JetSeek(_sesid, _tableid, SeekGrbit.SeekEQ);

                    var value = Api.RetrieveColumn(_sesid, _tableid, _secondaryColumnId);

                    Debug.Assert(value != null);

                    processed++;
                }

                sw.Stop();

                return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = processed,
                    Memory = GetMemory()
                };
            }
            finally
            {
                Close();
            }
        }

        private void Close()
        {
            Api.JetCloseTable(_sesid, _tableid);
            Api.JetEndSession(_sesid, EndSessionGrbit.None);
            Api.JetTerm(_instance);
        }
    }
}