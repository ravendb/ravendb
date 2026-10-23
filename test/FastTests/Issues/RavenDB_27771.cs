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

namespace FastTests.Issues;

public class RavenDB_27771(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void NewTermsInsertedBetweenExistingOnesAreStoredOnce()
    {
        const int termsPerBatch = 20_000;
        Slice.From(Allocator, "id()", out Slice idSlice);
        Slice.From(Allocator, "Name", out Slice nameSlice);
        using var fields = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice)
            .AddBinding(1, nameSlice)
            .Build();

        // the second batch puts new terms between the existing ones, so it splits pages in the middle of the tree
        IndexTerms(fields, parity: 0, termsPerBatch);
        IndexTerms(fields, parity: 1, termsPerBatch);

        using var tx = Env.ReadTransaction();
        using var searcher = new IndexSearcher(tx, fields);
        var nameTermIds = AssertTreeHoldsExactly(tx, nameSlice, Term, 2 * termsPerBatch);
        var idTermIds = AssertTreeHoldsExactly(tx, idSlice, Id, 2 * termsPerBatch);

        var nameEntriesToTerms = searcher.EntriesToTermsReader(nameSlice);
        var idEntriesToTerms = searcher.EntriesToTermsReader(idSlice);
        var ids = new long[4];
        for (int i = 0; i < 2 * termsPerBatch; i++)
        {
            Assert.Equal(1, searcher.TermQuery("Name", Term(i)).Fill(ids));
            long entryId = ids[0];
            Assert.Equal(1, searcher.TermQuery("id()", Id(i)).Fill(ids));
            Assert.Equal(entryId, ids[0]);

            // sorting reads the term of an entry from entries-to-terms, it has to be the term stored in the tree
            Assert.True(nameEntriesToTerms.TryGetValue(entryId, out long nameTermId));
            Assert.Equal(nameTermIds[Term(i)], nameTermId);
            Assert.True(idEntriesToTerms.TryGetValue(entryId, out long idTermId));
            Assert.Equal(idTermIds[Id(i)], idTermId);
        }
    }

    private static Dictionary<string, long> AssertTreeHoldsExactly(Transaction tx, Slice field, Func<int, string> termFor, int count)
    {
        var tree = tx.ReadTree(global::Corax.Constants.IndexWriter.FieldsSlice).CompactTreeFor(field);
        tree.VerifyOrderOfElements();
        Assert.Equal(count, tree.NumberOfEntries);

        var termsContainer = new Container(tx.LowLevelTransaction.GetPage((long)tree._inner.State.TermsContainerId));
        // a container starts with 4 internal items, every term is stored once
        Assert.Equal(count + 4, termsContainer.GetNumberOfEntries());

        var expected = Enumerable.Range(0, count).Select(termFor).Order(StringComparer.Ordinal).ToList();
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
        Assert.Equal(count, termIds.Count);
        return termIds;
    }

    private void IndexTerms(IndexFieldsMapping fields, int parity, int count)
    {
        using var writer = new IndexWriter(Env, fields, SupportedFeatures.All);
        for (int i = 0; i < count; i++)
        {
            var term = 2 * i + parity;
            using var entry = writer.Index(Encoding.UTF8.GetBytes(Id(term)));
            entry.Write(0, Encoding.UTF8.GetBytes(Id(term)));
            entry.Write(1, Encoding.UTF8.GetBytes(Term(term)));
            entry.EndWriting();
        }
        writer.Commit();
    }

    private static string Term(int i) => $"term-{i:D8}";

    private static string Id(int i) => $"items/{i}";


    private long CreateTreeAndGetRootPage()
    {
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("test");
            tree.Add("key", "value");
            tx.Commit();
        }

        using (var tx = Env.ReadTransaction())
            return tx.ReadTree("test").State.Header.RootPageNumber;
    }
}
