using System;
using System.Linq;
using Sparrow.Binary;
using Voron;
using Voron.Data.Tables;
using Voron.Global;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class TableRenaming : StorageTest
    {
        private const string OldTableName = "Items";
        private readonly string _newTableName = $"{OldTableName}_new";

        [Fact]
        public unsafe void CanRenameTable()
        {
            using (var tx = Env.WriteTransaction())
            using (Slice.From(tx.Allocator, "val1", out var key))
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                Slice.From(tx.Allocator, "IndexNumber", out var indexNumber);

                var index = new TableSchema.SchemaIndexDef
                {
                    Name = indexNumber,
                    StartIndex = 0,
                    Count = 1
                };
                var fixedSizedIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    Name = etagIndexName,
                    StartIndex = 1
                };

                var tableSchema = new TableSchema()
                    .DefineIndex(index)
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 0,
                        Count = 1
                    });

                tableSchema.Create(tx, OldTableName, 16);
                var itemsTable = tx.OpenTable(tableSchema, OldTableName);
                const long number = 1L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number));
                    itemsTable.Set(builder);
                }

                var oldTableReport = itemsTable.GetReport(true);
                var numberOfEntries = itemsTable.NumberOfEntries;
                var fixedSizeEntries = itemsTable.GetNumberOfEntriesAfter(fixedSizedIndex, 0, out var totalCount);

                var indexResults = itemsTable.SeekForwardFrom(index, key, 0).ToList();
                var indexResultsCount = indexResults.Count;
                var ptr = indexResults[0].Result.Reader.Read(1, out var valueSize);
                var valueInIndex = *(long*)ptr;

                var renamedTable = tx.RenameTable(OldTableName, _newTableName, doSchemaValidation: true);

                Assert.Equal(numberOfEntries, renamedTable.NumberOfEntries);
                Assert.Equal(_newTableName, renamedTable.Name.ToString());

                var renamedFixedSizeEntries = renamedTable.GetNumberOfEntriesAfter(fixedSizedIndex, 0, out var renamedTotalCount);
                Assert.Equal(fixedSizeEntries, renamedFixedSizeEntries);
                Assert.Equal(totalCount, renamedTotalCount);

                var newTableReport = renamedTable.GetReport(true);

                Assert.Equal(oldTableReport.Indexes.Count, newTableReport.Indexes.Count);
                Assert.Equal(oldTableReport.Indexes[0].Name, newTableReport.Indexes[0].Name);
                Assert.Equal(oldTableReport.Indexes[0].NumberOfEntries, newTableReport.Indexes[0].NumberOfEntries);
                Assert.Equal(oldTableReport.Indexes[0].Type, newTableReport.Indexes[0].Type);
                Assert.Equal(oldTableReport.Indexes[1].Name, newTableReport.Indexes[1].Name);
                Assert.Equal(oldTableReport.Indexes[1].NumberOfEntries, newTableReport.Indexes[1].NumberOfEntries);
                Assert.Equal(oldTableReport.Indexes[1].Type, newTableReport.Indexes[1].Type);
                Assert.Equal(oldTableReport.Indexes[2].Name, newTableReport.Indexes[2].Name);
                Assert.Equal(oldTableReport.Indexes[2].NumberOfEntries, newTableReport.Indexes[2].NumberOfEntries);
                Assert.Equal(oldTableReport.Indexes[2].Type, newTableReport.Indexes[2].Type);

                indexResults = itemsTable.SeekForwardFrom(index, key, 0).ToList();
                var renamedIndexResultsCount = indexResults.Count;
                ptr = indexResults[0].Result.Reader.Read(1, out var renamedValueSize);
                var renamedValueInIndex = *(long*)ptr;
                Assert.Equal(indexResultsCount, renamedIndexResultsCount);
                Assert.Equal(valueSize, renamedValueSize);
                Assert.Equal(valueInIndex, renamedValueInIndex);
            }
        }

        [Fact]
        public void ShouldPreventFromRenamingTableInReadTransaction()
        {
            using (var tx = Env.ReadTransaction())
            {
                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable(OldTableName, _newTableName));

                Assert.Equal("Cannot rename a table with a read only transaction", ae.Message);
            }
        }

        [Fact]
        public void MustNotRenameToRootAndFreeSpaceRootTrees()
        {
            using (var tx = Env.WriteTransaction())
            {
                var ex = Assert.Throws<InvalidOperationException>(() => tx.RenameTable(OldTableName, Constants.RootTreeName));
                Assert.Equal($"Cannot create a table with reserved name: {Constants.RootTreeName}", ex.Message);
            }
        }

        [Fact]
        public void ShouldNotAllowToRenameTableIfTableAlreadyExists()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tableSchema = new TableSchema()
                    .DefineKey(new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, OldTableName, 16);
                tableSchema.Create(tx, _newTableName, 16);

                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable(OldTableName, _newTableName));
                Assert.Equal($"Cannot rename a table with the name of an existing one: {_newTableName}", ae.Message);
            }
        }

        [Fact]
        public void ShouldThrowIfTableDoesNotExist()
        {
            using (var tx = Env.WriteTransaction())
            {
                var ae = Assert.Throws<ArgumentException>(() => tx.RenameTable(OldTableName, _newTableName));
                Assert.Equal($"Table {OldTableName} does not exists", ae.Message);
            }
        }
    }
}
