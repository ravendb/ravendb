using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace SlowTests.Issues;

public class RavenDB_27771(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void NewTermsInsertedBetweenExistingOnesAreStoredOnce()
    {
        const int termsPerBatch = 20_000;
        using var fields = CreateFields(out Slice idSlice, out Slice nameSlice);

        // the second batch puts new terms between the existing ones, so it splits pages in the middle of the tree
        IndexTerms(fields, parity: 0, termsPerBatch);
        IndexTerms(fields, parity: 1, termsPerBatch);

        using var tx = Env.ReadTransaction();
        using var searcher = new IndexSearcher(tx, fields);
        var all = Enumerable.Range(0, 2 * termsPerBatch).ToList();
        var nameTermIds = AssertTreeHoldsExactly(tx, nameSlice, all.Select(Term));
        var idTermIds = AssertTreeHoldsExactly(tx, idSlice, all.Select(Id));
        AssertNumericTreeHoldsExactly(tx, all);

        // sorting reads the term of an entry from entries-to-terms, it has to be the term stored in the tree
        var nameEntriesToTerms = searcher.EntriesToTermsReader(nameSlice);
        var idEntriesToTerms = searcher.EntriesToTermsReader(idSlice);
        var ids = new long[4];
        foreach (int i in all)
        {
            Assert.Equal(1, searcher.TermQuery("id()", Id(i)).Fill(ids));
            Assert.True(nameEntriesToTerms.TryGetValue(ids[0], out long nameTermId));
            Assert.Equal(nameTermIds[Term(i)], nameTermId);
            Assert.True(idEntriesToTerms.TryGetValue(ids[0], out long idTermId));
            Assert.Equal(idTermIds[Id(i)], idTermId);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ConsecutiveTermsDeletedInOneBatchAreRemovedFromTrees()
    {
        const int count = 20_000;
        using var fields = CreateFields(out Slice idSlice, out Slice nameSlice);
        IndexTerms(fields, parity: 0, count);

        // the deleted terms and numbers are next to each other in their trees, so they are removed as runs
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            for (int i = 2_000; i < 12_000; i++)
                Assert.True(writer.TryDeleteEntry(Id(2 * i)));
            writer.Commit();
        }

        var remaining = Enumerable.Range(0, count).Where(i => i < 2_000 || i >= 12_000).Select(i => 2 * i).ToList();
        using var tx = Env.ReadTransaction();
        AssertTreeHoldsExactly(tx, nameSlice, remaining.Select(Term));
        AssertTreeHoldsExactly(tx, idSlice, remaining.Select(Id));
        AssertNumericTreeHoldsExactly(tx, remaining);
    }

    private IndexFieldsMapping CreateFields(out Slice idSlice, out Slice nameSlice)
    {
        Slice.From(Allocator, "id()", out idSlice);
        Slice.From(Allocator, "Name", out nameSlice);
        return IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice)
            .AddBinding(1, nameSlice)
            .Build();
    }

    private static unsafe Dictionary<string, long> AssertTreeHoldsExactly(Transaction tx, Slice field, IEnumerable<string> terms)
    {
        var expected = terms.Order(StringComparer.Ordinal).ToList();
        var tree = tx.ReadTree(global::Corax.Constants.IndexWriter.FieldsSlice).CompactTreeFor(field);
        tree.VerifyOrderOfElements();
        Assert.Equal(expected.Count, tree.NumberOfEntries);

        // a stored term is referenced by its leaf entry and by every branch that uses it as a separator (a separator may keep a deleted
        // term), its reference count must match them and the container must not hold anything else
        var references = new Dictionary<long, int>();
        foreach (var page in tree.AllPages())
        {
            foreach (var (key, _) in tree._inner.AllEntriesIn(page))
            {
                if (key.IsEmpty == false)
                    references[(long)key.ContainerId] = references.GetValueOrDefault((long)key.ContainerId) + 1;
            }
        }

        var items = Container.GetAllIds(tx.LowLevelTransaction, tree._inner.State.TermsContainerId);
        Assert.Equal(references.Count, items.Count);
        foreach (var id in items)
        {
            Container.Get(tx.LowLevelTransaction, new ContainerEntryId(id), out var item);
            Assert.Equal(references[id], *item.Address & 0xF);
        }

        var termIds = new Dictionary<string, long>();
        using var scope = new CompactKeyCacheScope(tx.LowLevelTransaction);
        var it = tree._inner.Iterate<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>();
        it.Reset();
        while (it.MoveNext(out CompactTree.CompactKeyLookup key, out _, out _))
        {
            key.FillKey(scope.Key, tree._inner);
            var term = scope.Key.ToString();
            Assert.Equal(expected[termIds.Count], term);
            termIds.Add(term, (long)key.ContainerId);
        }
        Assert.Equal(expected.Count, termIds.Count);
        return termIds;
    }

    private static void AssertNumericTreeHoldsExactly(Transaction tx, IEnumerable<int> numbers)
    {
        using var nameScope = Slice.From(tx.Allocator, "Name-L", out Slice name);
        var tree = tx.ReadTree(global::Corax.Constants.IndexWriter.FieldsSlice).LookupFor<Int64LookupKey>(name);
        tree.VerifyStructure();
        var expected = numbers.Order().ToList();
        Assert.Equal(expected.Count, tree.NumberOfEntries);

        var it = tree.Iterate();
        it.Reset();
        foreach (long number in expected)
        {
            Assert.True(it.MoveNext(out Int64LookupKey key, out _, out _));
            Assert.Equal(number, key.Value);
        }
        Assert.False(it.MoveNext(out Int64LookupKey _, out _, out _));
    }

    private void IndexTerms(IndexFieldsMapping fields, int parity, int count)
    {
        using var writer = new IndexWriter(Env, fields, SupportedFeatures.All);
        for (int i = 0; i < count; i++)
        {
            var term = 2 * i + parity;
            using var entry = writer.Index(Encoding.UTF8.GetBytes(Id(term)));
            entry.Write(0, Encoding.UTF8.GetBytes(Id(term)));
            entry.Write(1, Encoding.UTF8.GetBytes(Term(term)), term, term);
            entry.EndWriting();
        }
        writer.Commit();
    }

    private static string Term(int i) => $"term-{i:D8}";

    private static string Id(int i) => $"items/{i}";
}
