using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class IndexingWritePathAllocations(ITestOutputHelper output) : RavenLowLevelTestBase(output)
    {
        private const int NumberOfDocuments = 2_000;
        private const string Collection = "Orders";

        private static readonly string CollectionTreeName = IndexStorage.Constants.DocumentReferenceCollectionPrefix + Collection;

        // Before RavenDB-27880 writing the references of a batch allocated two lists per key, a dictionary entry, a sort delegate
        // and a tree iterator per referencing document, ~660 bytes per document with two references each.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Writing_unchanged_references()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);
                    WriteReferences(references, tx, ids, referenced);
                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);

                    // a new container per batch, as CurrentIndexingScope does
                    var bytes = MeasurePerDocument(() => WriteReferences(references, tx, ids, referenced));
                    AssertAtMost(400, "Write unchanged references", bytes);
                }
            }
        }

        // Before RavenDB-27880 removing the references of a deleted document allocated ~130 bytes on top of the tree operations
        // (a list, its array and the name of the collection tree).
        [RavenFact(RavenTestCategory.Indexes)]
        public void Removing_references_of_deleted_documents()
        {
            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out var referenced);
                    WriteReferences(references, tx, ids, referenced);
                    tx.Commit();
                }

                double treeOperations;
                using (var tx = context.OpenWriteTransaction())
                {
                    var allocator = tx.InnerTransaction.Allocator;
                    var referencesTree = tx.InnerTransaction.ReadTree(IndexStorage.IndexSchema.References);
                    var collectionTree = tx.InnerTransaction.ReadTree(CollectionTreeName);
                    var buffer = new Slice[16];
                    CreateReferenceKeys(allocator, out var ids, out _);

                    treeOperations = MeasureRemoval(i => RemoveWithTreeOperations(referencesTree, collectionTree, ids[i], buffer, allocator));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    CreateReferenceKeys(tx.InnerTransaction.Allocator, out var ids, out _);

                    var bytes = MeasureRemoval(i => references.RemoveReferences(ids[i], Collection, tx));
                    Assert.Equal((0, 0), references.GetReferenceTablesCount(Collection, tx));

                    Output.WriteLine($"{"Tree operations of a removal",-48} {treeOperations,8:F1} bytes/doc");
                    AssertAtMost(32, "Remove references, over the tree operations", bytes - treeOperations);
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Rewriting_references_keeps_only_the_current_ones()
        {
            var many = Enumerable.Range(0, 300).Select(i => $"items/{i:D4}").ToArray();
            var even = many.Where((_, i) => i % 2 == 0).ToArray();

            using (var database = CreateDocumentDatabase())
            using (var index = CreateMapIndex(database))
            using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var references = index._indexStorage.ReferencesForDocuments;

                using (var tx = context.OpenWriteTransaction())
                {
                    WriteReferences(references, tx, new Dictionary<string, string[]>
                    {
                        ["orders/a"] = ["companies/3", "companies/1", "companies/2"],
                        ["orders/b"] = ["companies/2"],
                        ["orders/c"] = ["companies/1", "companies/4", "companies/1"],
                        ["orders/d"] = many
                    });
                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    AssertReferences(tx, "orders/a", "companies/1", "companies/2", "companies/3");
                    AssertReferences(tx, "orders/b", "companies/2");
                    AssertReferences(tx, "orders/c", "companies/1", "companies/4");
                    AssertReferences(tx, "orders/d", many);
                    AssertReferencedBy(tx, "companies/1", "orders/a", "orders/c");
                    AssertReferencedBy(tx, "companies/2", "orders/a", "orders/b");
                    AssertReferencedBy(tx, "items/0001", "orders/d");

                    // orders/b is not in this batch
                    WriteReferences(references, tx, new Dictionary<string, string[]>
                    {
                        ["orders/a"] = ["companies/5", "companies/2"],
                        ["orders/c"] = ["companies/4"],
                        ["orders/d"] = even
                    });

                    AssertReferences(tx, "orders/a", "companies/2", "companies/5");
                    AssertReferences(tx, "orders/b", "companies/2");
                    AssertReferences(tx, "orders/c", "companies/4");
                    AssertReferences(tx, "orders/d", even);
                    AssertReferencedBy(tx, "companies/1");
                    AssertReferencedBy(tx, "companies/2", "orders/a", "orders/b");
                    AssertReferencedBy(tx, "companies/3");
                    AssertReferencedBy(tx, "companies/4", "orders/c");
                    AssertReferencedBy(tx, "companies/5", "orders/a");
                    AssertReferencedBy(tx, "items/0000", "orders/d");
                    AssertReferencedBy(tx, "items/0001");

                    using (Slice.From(tx.InnerTransaction.Allocator, "orders/a", out Slice a))
                    using (Slice.From(tx.InnerTransaction.Allocator, "orders/d", out Slice d))
                    {
                        references.RemoveReferences(a, Collection, tx);
                        references.RemoveReferences(d, Collection, tx);
                    }

                    AssertReferences(tx, "orders/a");
                    AssertReferences(tx, "orders/d");
                    AssertReferencedBy(tx, "companies/2", "orders/b");
                    AssertReferencedBy(tx, "companies/5");
                    AssertReferencedBy(tx, "items/0000");
                    Assert.Equal((2, 2), references.GetReferenceTablesCount(Collection, tx));
                }
            }
        }

        private static AutoMapIndex CreateMapIndex(Raven.Server.Documents.DocumentDatabase database)
        {
            return AutoMapIndex.CreateNew(new AutoMapIndexDefinition(Collection, [new AutoIndexField { Name = "Name", Storage = FieldStorage.No }]), database);
        }

        private static void AssertReferences(RavenTransaction tx, string key, params string[] expected) =>
            AssertMultiValues(tx, tx.InnerTransaction.ReadTree(IndexStorage.IndexSchema.References), key, expected);

        private static void AssertReferencedBy(RavenTransaction tx, string key, params string[] expected) =>
            AssertMultiValues(tx, tx.InnerTransaction.ReadTree(CollectionTreeName), key, expected);

        private static void AssertMultiValues(RavenTransaction tx, Tree tree, string key, string[] expected)
        {
            var actual = new List<string>();
            using (Slice.From(tx.InnerTransaction.Allocator, key, out Slice keySlice))
            using (var it = tree.MultiRead(keySlice))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        actual.Add(it.CurrentKey.ToString());
                    } while (it.MoveNext());
                }
            }

            Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal), actual);
        }

        private static void WriteReferences(IndexStorage.ReferencesBase references, RavenTransaction tx, Dictionary<string, string[]> referencesByDocument)
        {
            var container = new ReferenceContainer();
            foreach (var (id, referenced) in referencesByDocument)
            {
                foreach (var value in referenced)
                {
                    // a slice per call, as LoadDocument does
                    Slice.From(tx.InnerTransaction.Allocator, id, ByteStringType.Immutable, out Slice idSlice);
                    Slice.From(tx.InnerTransaction.Allocator, value, ByteStringType.Immutable, out Slice valueSlice);
                    container.Add(idSlice, valueSlice);
                }
            }

            references.WriteReferencesForSingleCollection(Collection, container, tx);
        }

        private static void WriteReferences(IndexStorage.ReferencesBase references, RavenTransaction tx, Slice[] ids, Slice[][] referenced)
        {
            var container = new ReferenceContainer();
            for (int i = 0; i < ids.Length; i++)
            {
                foreach (var value in referenced[i])
                    container.Add(ids[i], value);
            }

            references.WriteReferencesForSingleCollection(Collection, container, tx);
        }

        private static void RemoveWithTreeOperations(Tree referencesTree, Tree collectionTree, Slice id, Slice[] buffer, ByteStringContext allocator)
        {
            int count = 0;
            using (var it = referencesTree.MultiRead(id))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return;

                do
                {
                    buffer[count++] = it.CurrentKey.Clone(allocator, ByteStringType.Immutable);
                } while (it.MoveNext());
            }

            for (int i = 0; i < count; i++)
            {
                referencesTree.MultiDelete(id, buffer[i]);
                collectionTree.MultiDelete(buffer[i], id);
                buffer[i].Release(allocator);
            }
        }

        private static Slice[] CreateDocumentIds(ByteStringContext allocator)
        {
            var ids = new Slice[NumberOfDocuments];
            for (int i = 0; i < NumberOfDocuments; i++)
                Slice.From(allocator, $"orders/{i:D8}", ByteStringType.Immutable, out ids[i]);
            return ids;
        }

        private static void CreateReferenceKeys(ByteStringContext allocator, out Slice[] ids, out Slice[][] referenced)
        {
            ids = CreateDocumentIds(allocator);
            referenced = new Slice[NumberOfDocuments][];
            for (int i = 0; i < NumberOfDocuments; i++)
            {
                Slice.From(allocator, $"companies/{i % 100:D8}", ByteStringType.Immutable, out var company);
                Slice.From(allocator, $"employees/{i % 30:D8}", ByteStringType.Immutable, out var employee);
                referenced[i] = [company, employee];
            }
        }

        private static double MeasurePerDocument(Action batch)
        {
            batch(); // warm up so JIT and pool filling are excluded from the measurement

            const int rounds = 3;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < rounds; i++)
                batch();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (rounds * NumberOfDocuments);
        }

        // each document can be removed once, so the first ones warm up and the rest are measured
        private static double MeasureRemoval(Action<int> remove)
        {
            const int warmup = 100;
            for (int i = 0; i < warmup; i++)
                remove(i);

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = warmup; i < NumberOfDocuments; i++)
                remove(i);

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (NumberOfDocuments - warmup);
        }

        private void AssertAtMost(int limit, string name, double bytesPerDocument)
        {
            Output.WriteLine($"{name,-48} {bytesPerDocument,8:F1} bytes/doc");
#if !DEBUG // debug-only checks allocate too
            Assert.True(bytesPerDocument <= limit, $"{name} allocated {bytesPerDocument:F1} bytes/doc, expected at most {limit}");
#endif
        }
    }
}
