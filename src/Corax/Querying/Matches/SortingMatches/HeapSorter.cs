using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron.Util;
#if DEBUG
using Newtonsoft.Json;
#endif

namespace Corax.Querying.Matches.SortingMatches;

internal static class HeapSorterBuilder
{
    public static unsafe NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> BuildCompoundCompactKeySorter<TSecondaryComparer>(Span<int> documents,
        Span<UnmanagedSpan> terms, bool descending, TSecondaryComparer secondaryCmp)
        where TSecondaryComparer : IComparer<int>
    {
        static int Ascending(ref NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> sorter, UnmanagedSpan termA, int posA, UnmanagedSpan termB, int posB)
        {
            var cmp = CompactKeyComparer.Compare(termA, termB);
            return cmp == 0 ? 
                sorter.SecondaryComparer.Compare(posA, posB) 
                : cmp;
        }

        static int Descending(ref NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer> sorter, UnmanagedSpan termA, int posA, UnmanagedSpan termB, int posB)
        {
            //In first comparer we control the order of parameters, however secondary comparer (and it's inners have to be wrapped in Descending<>)
            var cmp = CompactKeyComparer.Compare(termB, termA);
            return cmp == 0 ? 
                sorter.SecondaryComparer.Compare(posA, posB) 
                : cmp;
        }

        var sorter = new NumericalMaxHeapSorter<UnmanagedSpan, TSecondaryComparer>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, secondaryCmp);
        return sorter;
    }


    public static unsafe TextualMaxHeapSorter<SkipSecondaryComparer> BuildSingleAlphanumericalSorter(Span<int> documents, Span<ByteString> terms,
        ByteStringContext allocator, bool descending)
    {
        static int CompareAlphanumericalAscending(ref TextualMaxHeapSorter<SkipSecondaryComparer> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB,
            int posB)
        {
            return AlphanumericalComparer.Instance.Compare(termA, termB);
        }

        static int CompareAlphanumericalDescending(ref TextualMaxHeapSorter<SkipSecondaryComparer> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB,
            int posB)
        {
            return AlphanumericalComparer.Instance.Compare(termB, termA);
        }

        var sorter = new TextualMaxHeapSorter<SkipSecondaryComparer>();
        sorter.Init(documents, terms, allocator, descending, descending ? &CompareAlphanumericalDescending : &CompareAlphanumericalAscending, default);
        return sorter;
    }

    public static unsafe TextualMaxHeapSorter<TSecondaryCmp> BuildCompoundAlphanumericalSorter<TSecondaryCmp>(Span<int> documents, Span<ByteString> terms,
        ByteStringContext allocator, bool descending, TSecondaryCmp secondaryCmp) where TSecondaryCmp : IComparer<int>
    {
        static int CompareAlphanumericalAscending(ref TextualMaxHeapSorter<TSecondaryCmp> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB, int posB)
        {
            var result = AlphanumericalComparer.Instance.Compare(termA, termB);
            return result == 0 ? sorter.SecondaryComparer.Compare(posA, posB) : result;
        }

        static int CompareAlphanumericalDescending(ref TextualMaxHeapSorter<TSecondaryCmp> sorter, ReadOnlySpan<byte> termA, int posA, ReadOnlySpan<byte> termB, int posB)
        {
            //note reversed elements, only for the first comparer, inner comparers should be wrapped in Descending<T>
            var result = AlphanumericalComparer.Instance.Compare(termB, termA);
            return result == 0 ? sorter.SecondaryComparer.Compare(posA, posB) : result;
        }

        var sorter = new TextualMaxHeapSorter<TSecondaryCmp>();
        sorter.Init(documents, terms, allocator, descending, descending ? &CompareAlphanumericalDescending : &CompareAlphanumericalAscending, secondaryCmp);
        return sorter;
    }

    public static unsafe NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> BuildSingleNumericalSorter<TTermType>(Span<int> documents, Span<TTermType> terms,
        bool descending) where TTermType : unmanaged, IComparable
    {
        static int Ascending(ref NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            return termA.CompareTo(termB);
        }

        static int Descending(ref NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            return termB.CompareTo(termA);
        }

        var sorter = new NumericalMaxHeapSorter<TTermType, SkipSecondaryComparer>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, default);
        return sorter;
    }

    public static unsafe NumericalMaxHeapSorter<TTermType, TSecondaryCmp> BuildCompoundNumericalSorter<TTermType, TSecondaryCmp>(Span<int> documents,
        Span<TTermType> terms, bool descending, TSecondaryCmp secondaryCmp)
        where TSecondaryCmp : IComparer<int>
        where TTermType : unmanaged, IComparable
    {
        static int Ascending(ref NumericalMaxHeapSorter<TTermType, TSecondaryCmp> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            var cmp = termA.CompareTo(termB);
            return cmp == 0
                ? sorter.SecondaryComparer.Compare(posA, posB)
                : cmp;
        }

        static int Descending(ref NumericalMaxHeapSorter<TTermType, TSecondaryCmp> sorter, TTermType termA, int posA, TTermType termB, int posB)
        {
            var cmp = termB.CompareTo(termA);
            return cmp == 0
                ? sorter.SecondaryComparer.Compare(posA, posB)
                : cmp;
        }

        var sorter = new NumericalMaxHeapSorter<TTermType, TSecondaryCmp>();
        sorter.Init(documents, terms, null, descending, descending ? &Descending : &Ascending, secondaryCmp);
        return sorter;
    }

    internal struct SkipSecondaryComparer : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            throw new NotImplementedException("Used as marker for generics. Should never ever be called!");
        }
    }
}

internal unsafe ref struct NumericalMaxHeapSorter<TTermType, TSecondaryComparer> where TSecondaryComparer : IComparer<int>
    where TTermType : unmanaged
{
    private Span<int> _documents;
    private Span<TTermType> _terms;
    private int _heapSize;
    private int _heapCapacity; 
    public bool IsDescending;
    public TSecondaryComparer SecondaryComparer;


    // this, termA, posA, termB, posB
    private delegate*<ref NumericalMaxHeapSorter<TTermType, TSecondaryComparer>, TTermType, int, TTermType, int, int> _compare;

    public void Init(Span<int> documents, Span<TTermType> terms, ByteStringContext allocator, bool descending,
        delegate*<ref NumericalMaxHeapSorter<TTermType, TSecondaryComparer>, TTermType, int, TTermType, int, int> compare, TSecondaryComparer secondaryCmp)
    {
        IsDescending = descending;
        _documents = documents;
        _terms = terms;
        _heapCapacity = documents.Length;
        _compare = compare;
        SecondaryComparer = secondaryCmp;
    }

    public void Insert(int document, TTermType newTerm)
    {
        if (_heapSize < _heapCapacity)
        {
            _documents[_heapSize] = document;
            _terms[_heapSize] = newTerm;
            _heapSize++;
            HeapIncreaseKey(_heapSize - 1);
            return;
        }


        // Since we're gathering the smallest N keys, we can do a simple comparison cmp(Max_Heap, New),
        // as every new item will have to be smaller than the maximum in the heap.
        int isNewTermSmallerThanCurrentMax = Compare(_terms[0], _documents[0], newTerm, document);

        if (isNewTermSmallerThanCurrentMax > 0)
        {
            ReplaceMax(newTerm, document);
        }
    }

    private int Compare(TTermType xDoc, int xIndex, TTermType yDoc, int yIndex)
    {
        var isNewTermSmallerThanCurrentMax = _compare(ref this, xDoc, xIndex, yDoc, yIndex);
        if (isNewTermSmallerThanCurrentMax == 0)
        {
            isNewTermSmallerThanCurrentMax = xIndex - yIndex;
        }

        return isNewTermSmallerThanCurrentMax;
    }

    private void HeapIncreaseKey(int i)
    {
        int parent = Parent(i);

        while (i != parent && parent >= 0 && Compare( _terms[parent], _documents[parent], _terms[i], _documents[i]) < 0)
        {
            Swap(parent, i);
            i = parent;
            parent = Parent(i);
        }
    }

    private void MaxHeapify(int i)
    {
        Heapify:
        int largest = i;
        int leftChild = LeftChild(i);
        int rightChild = RightChild(i);

        // left child > current
        if (leftChild < _heapSize)
        {
            var cmp = Compare(_terms[leftChild], _documents[leftChild], _terms[i], _documents[i]);

            largest = cmp > 0
                ? leftChild
                : i;
        }

        // right child > largest
        if (rightChild < _heapSize)
        {
            var cmp = Compare(_terms[rightChild], _documents[rightChild], _terms[largest], _documents[largest]);

            if (cmp > 0)
                largest = rightChild;
        }

        if (largest != i)
        {
            Swap(largest, i);

            // Instead going recursively just jump to the beginning
            i = largest;
            goto Heapify;
        }
    }

    private void ReplaceMax(TTermType term, int documentId)
    {
        _terms[0] = term;
        _documents[0] = documentId;
        MaxHeapify(0);
    }

    private void RemoveMax()
    {
        _terms[0] = _terms[_heapSize - 1];
        _documents[0] = _documents[_heapSize - 1];
        _heapSize--;

        MaxHeapify(0);
    }

    public void Fill(Span<long> batchResults, ref ContextBoundNativeList<long> results, ref ContextBoundNativeList<float> scoreDestination, Span<float> scores)
    {
        ValidateMaxHeapStructure();
        var start = results.Count;
        results.EnsureCapacityFor(_heapSize);
        int documentsToReturn = _heapSize;

        var exposeScores = scoreDestination.HasContext && scores.Length != 0;
        if (exposeScores)
            scoreDestination.EnsureCapacityFor(_heapSize);
        
        while (documentsToReturn > 0)
        {
            results.AddUnsafe(batchResults[_documents[0]]);
            
            if (exposeScores)
                scoreDestination.AddUnsafe(scores[_documents[0]]);
            
            RemoveMax();
            documentsToReturn--;
        }

        Debug.Assert(_heapSize == 0, "_heapSize == 0");
        // It's easier for us to operate on a MaxHeap, so we get the elements in descending order (locally).
        // However, instead of rebuilding the heap as a MinHeap, let's add everything from the heap and call Reverse from the Span extension,
        // which is a vectorized operation.
        results.ToSpan().Slice(start).Reverse();
    }

    public void FillWithTerms(Span<long> batchResults, ref ContextBoundNativeList<long> results, ref ContextBoundNativeList<TTermType> terms, ref ContextBoundNativeList<float> scoreDestination, Span<float> scores)
    {
        ValidateMaxHeapStructure();

        var total = _heapSize;
        var startDocuments = results.Count;
        var startTerms = terms.Count;

        var exposeScores = scoreDestination.HasContext && scores.Length != 0;
        if (exposeScores)
            scoreDestination.EnsureCapacityFor(_heapSize);
        
        results.EnsureCapacityFor(_heapSize);
        terms.EnsureCapacityFor(_heapSize);
        int documentsToReturn = _heapSize;

        while (documentsToReturn > 0)
        {
            results.AddUnsafe(batchResults[_documents[0]]);
            terms.AddUnsafe(_terms[0]);
            if (exposeScores)
                scoreDestination.AddUnsafe(scores[_documents[0]]);
            
            RemoveMax();
            documentsToReturn--;
        }

        Debug.Assert(_heapSize == 0, "_heapSize == 0");
        // It's easier for us to operate on a MaxHeap, so we get the elements in descending order (locally).
        // However, instead of rebuilding the heap as a MinHeap, let's add everything from the heap and call Reverse from the Span extension,
        // which is a vectorized operation.
        results.ToSpan().Slice(startDocuments, total).Reverse();
        terms.ToSpan().Slice(startTerms, total).Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Parent(int idX) => (idX - 1) / 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int LeftChild(int idX) => idX * 2 + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int RightChild(int idX) => idX * 2 + 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Swap(int a, int b)
    {
        (_documents[a], _documents[b]) = (_documents[b], _documents[a]);
        (_terms[a], _terms[b]) = (_terms[b], _terms[a]);
    }

#if DEBUG
    public string CreateGraph(Span<long> ids, IndexSearcher searcher)
    {
        var root = new Node();
        var queue = new Queue<(int NodeId, Node NodeObject)>();
        queue.Enqueue((0, root));
        var idReader = searcher.TermsReaderFor("id()");
        while (queue.TryDequeue(out var n))
        {
            var node = n.NodeId;
            if (node >= _heapSize)
                continue;

            var currentTerm = _terms[node];
            var leftChild = LeftChild(node);
            var rightChild = RightChild(node);
            var current = n.NodeObject;
            current.Value = currentTerm.ToString();
            current.DocumentId = idReader.GetTermFor(ids[_documents[node]]);
            
            if (leftChild < _heapSize)
            {
                if (_compare(ref this, currentTerm, _documents[node], _terms[leftChild], _documents[leftChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                var newNode = new Node();
                current.LeftChild = newNode;
                queue.Enqueue((leftChild, newNode));
            }

            if (rightChild < _heapSize)
            {
                if (_compare(ref this, currentTerm, _documents[node], _terms[rightChild], _documents[rightChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                var newNode = new Node();
                current.RightChild = newNode;
                queue.Enqueue((rightChild, newNode));
            }
        }

        return JsonConvert.SerializeObject(root);
    }
    
    
    private class Node
    {
        public Node LeftChild { get; set; }
        public Node RightChild { get; set; }
        public string Value { get; set; }
        public string DocumentId { get; set; }
    }
#endif

    
    [Conditional("DEBUG")]
    private void ValidateMaxHeapStructure()
    {
        var queue = new Queue<int>();
        queue.Enqueue(0);

        while (queue.TryDequeue(out var node))
        {
            if (node >= _heapSize)
                continue;

            var currentTerm = _terms[node];
            var leftChild = LeftChild(node);
            var rightChild = RightChild(node);

            if (leftChild < _heapSize)
            {
                if (Compare(currentTerm, _documents[node], _terms[leftChild], _documents[leftChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(leftChild);
            }

            if (rightChild < _heapSize)
            {
                if (Compare(currentTerm, _documents[node], _terms[rightChild], _documents[rightChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(rightChild);
            }
        }
    }
}

internal unsafe ref struct TextualMaxHeapSorter<TSecondaryComparer> where TSecondaryComparer : IComparer<int>
{
    private Span<int> _documents;
    private Span<ByteString> _terms;
    private int _heapSize;
    private int _heapCapacity;
    private ByteStringContext _allocator;
    public bool IsDescending;
    public TSecondaryComparer SecondaryComparer;


    // this, termA, posA, termB, posB
    private delegate*<ref TextualMaxHeapSorter<TSecondaryComparer>, ReadOnlySpan<byte>, int, ReadOnlySpan<byte>, int, int> _compare;

    public void Init(Span<int> documents, Span<ByteString> terms, ByteStringContext allocator, bool descending,
        delegate*<ref TextualMaxHeapSorter<TSecondaryComparer>, ReadOnlySpan<byte>, int, ReadOnlySpan<byte>, int, int> compare, TSecondaryComparer secondaryCmp)
    {
        IsDescending = descending;
        _allocator = allocator;
        _documents = documents;
        _terms = terms;
        _heapCapacity = documents.Length;
        _compare = compare;
        SecondaryComparer = secondaryCmp;
    }

    public void Insert(int document, ReadOnlySpan<byte> newTerm)
    {
        if (_heapSize < _heapCapacity)
        {
            _allocator.Allocate(newTerm.Length, out ByteString mem);
            newTerm.CopyTo(mem.ToSpan());

            _documents[_heapSize] = document;
            _terms[_heapSize] = mem;
            _heapSize++;
            HeapIncreaseKey(_heapSize - 1);
            return;
        }

        // Since we're gathering the smallest N keys, we can do a simple comparison cmp(Max_Heap, New),
        // as every new item will have to be smaller than the maximum in the heap.
        int isNewTermSmallerThanCurrentMax = Compare(_terms[0].ToSpan(), _documents[0], newTerm, document);

        if (isNewTermSmallerThanCurrentMax > 0)
        {
            _allocator.Allocate(newTerm.Length, out ByteString mem);
            newTerm.CopyTo(mem.ToSpan());

            ReplaceMax(mem, document);
        }
    }
    
    private void HeapIncreaseKey(int i)
    {
        int parent = Parent(i);

        while (parent >= 0 && Compare(_terms[parent].ToSpan(), _documents[parent], _terms[i].ToSpan(), _documents[i]) < 0)
        {
            Swap(parent, i);
            i = parent;
            parent = Parent(i);
        }
    }

    private void MaxHeapify(int i)
    {
        Heapify:
        int largest = i;
        int leftChild = LeftChild(i);
        int rightChild = RightChild(i);

        // left child > current
        if (leftChild < _heapSize)
        {
            var cmp = Compare(_terms[leftChild].ToSpan(), _documents[leftChild], _terms[i].ToSpan(), _documents[i]);

            largest = cmp > 0
                ? leftChild
                : i;
        }

        // right child > largest
        if (rightChild < _heapSize)
        {
            var cmp = Compare(_terms[rightChild].ToSpan(), _documents[rightChild], _terms[largest].ToSpan(), _documents[largest]);

            if (cmp > 0)
                largest = rightChild;
        }

        if (largest != i)
        {
            Swap(largest, i);

            // Instead going recursively just jump to the beginning
            i = largest;
            goto Heapify;
        }
    }

    private void ReplaceMax(ByteString term, int documentId)
    {
        _allocator.Release(ref _terms[0]);
        _terms[0] = term;
        _documents[0] = documentId;
        MaxHeapify(0);
    }

    private void RemoveMax()
    {
        _allocator.Release(ref _terms[0]);
        _terms[0] = _terms[_heapSize - 1];
        _documents[0] = _documents[_heapSize - 1];
        _heapSize--;

        MaxHeapify(0);
    }

    public void Fill(Span<long> batchResults, ref ContextBoundNativeList<long> results, ref ContextBoundNativeList<float> scoreDestination, Span<float> scores)
    {
        ValidateMaxHeapStructure();
        var start = results.Count;
        results.EnsureCapacityFor(_heapSize);
        int documentsToReturn = _heapSize;

        var exposeScore = scoreDestination.HasContext && scores.IsEmpty == false;
        if (exposeScore)
            scoreDestination.EnsureCapacityFor(_heapSize);
        
        while (documentsToReturn > 0)
        {
            results.AddUnsafe(batchResults[_documents[0]]);
            
            if (exposeScore)
                scoreDestination.AddUnsafe(scores[_documents[0]]);
            
            RemoveMax();
            documentsToReturn--;
        }

        Debug.Assert(_heapSize == 0, "_heapSize == 0");
        // It's easier for us to operate on a MaxHeap, so we get the elements in descending order (locally).
        // However, instead of rebuilding the heap as a MinHeap, let's add everything from the heap and call Reverse from the Span extension,
        // which is a vectorized operation.
        results.ToSpan().Slice(start).Reverse();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Parent(int idX) => (idX - 1) / 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int LeftChild(int idX) => idX * 2 + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int RightChild(int idX) => idX * 2 + 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Swap(int a, int b)
    {
        (_documents[a], _documents[b]) = (_documents[b], _documents[a]);
        (_terms[a], _terms[b]) = (_terms[b], _terms[a]);
    }
    
    private int Compare(ReadOnlySpan<byte> xDoc, int xIndex, ReadOnlySpan<byte> yDoc, int yIndex)
    {
        var isNewTermSmallerThanCurrentMax = _compare(ref this, xDoc, xIndex, yDoc, yIndex);
        if (isNewTermSmallerThanCurrentMax == 0)
        {
            isNewTermSmallerThanCurrentMax = xIndex - yIndex;
        }

        return isNewTermSmallerThanCurrentMax;
    }

    [Conditional("DEBUG")]
    private void ValidateMaxHeapStructure()
    {
        var queue = new Queue<int>();
        queue.Enqueue(0);

        while (queue.TryDequeue(out var node))
        {
            if (node >= _heapSize)
                continue;

            var currentTerm = _terms[node].ToSpan();
            var leftChild = LeftChild(node);
            var rightChild = RightChild(node);

            if (leftChild < _heapSize)
            {
                if (Compare(currentTerm, node, _terms[leftChild].ToSpan(), _documents[leftChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(leftChild);
            }

            if (rightChild < _heapSize)
            {
                if (Compare(currentTerm, _documents[node], _terms[rightChild].ToSpan(), _documents[rightChild]) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(IsDescending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(rightChild);
            }
        }
    }
}
