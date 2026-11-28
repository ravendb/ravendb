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
    public class IndexReferencesTests(ITestOutputHelper output) : RavenLowLevelTestBase(output)
    {
        private const string Collection = "Orders";

        private static readonly string CollectionTreeName = IndexStorage.Constants.DocumentReferenceCollectionPrefix + Collection;

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

    }
}
