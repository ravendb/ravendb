using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron.Util;
#if DEBUG
using Newtonsoft.Json;
#endif

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Textual heap sorter built for Corax's sorting primitives. It is not supposed to be used for sorting anything else.
/// </summary>
/// <typeparam name="TTermType">Unmanaged (value) type of terms</typeparam>
/// <typeparam name="TSecondaryComparer">Secondary comparer type. It takes indexes of documents inside batch results.</typeparam>
internal unsafe ref struct TextualMaxHeapSorter<TSecondaryComparer> where TSecondaryComparer : IComparer<int>
{
    /// <summary>Values are indexes of documents inside batchResult, not the documents themselves.</summary>
    private Span<int> _documents;
    
    // Terms from documents. We're creating a clone of them since we may need them later. 
    // The terms in CompactKey are encoded, and decoding uses temporary memory.
    private Span<ByteString> _terms;
    private int _heapSize;
    private int _heapCapacity;
    private ByteStringContext _allocator;
    public bool IsDescending;
    public TSecondaryComparer SecondaryComparer;


    /// <summary>
    /// A pointer to compare method.
    /// Params:
    /// this, termA, posA, termB, posB 
    /// </summary>
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

    /// <summary>
    /// Insert new document to the heap
    /// </summary>
    /// <param name="document">Index of document in batchResult</param>
    /// <param name="newTerm">term from document</param>
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

    /// <summary>
    /// Get sorted results with associated score (when requested).
    /// </summary>
    /// <param name="batchResults">An array with document ids</param>
    /// <param name="results">Destination of sorted results</param>
    /// <param name="scoreDestination">Destination of scores (sorted)</param>
    /// <param name="scores">An array with scores associated with batchResults</param>
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
        
        // When we sort a max-heap by repeatedly extracting the maximum value (at index 0), the result is in reverse order (locally - in the heap).
        // Note that we're not dealing with all documents, but only up to heapSize.
        // This structure allows us to make a simple and predictable comparison with the max element 
        // (which is the minimum in the case of descending sorting) to decide if a document
        // should replace the max element in the heap (and then find the new maximum).
        // Reversing the elements is done via Span<T>.Reverse, which is a vectorized operation.
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
        
        // When documents are equal, we have to stabilize the sort (for paging), so the final order is based on position in batchResults.
        if (isNewTermSmallerThanCurrentMax == 0)
        {
            isNewTermSmallerThanCurrentMax = xIndex - yIndex;
        }

        return isNewTermSmallerThanCurrentMax;
    }

    /// <summary>
    /// We're crawling through the data and asserting if it is a max-heap.
    /// </summary>
    /// <exception cref="InvalidDataException">When structure is not max-heap</exception>
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
                if (Compare(currentTerm, _documents[node], _terms[leftChild].ToSpan(), _documents[leftChild]) < 0)
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
