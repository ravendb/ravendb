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
/// Numerical heap sorter built for Corax's sorting primitives. It is not supposed to be used for sorting anything else.
/// </summary>
/// <typeparam name="TTermType">Unmanaged (value) type of terms</typeparam>
/// <typeparam name="TSecondaryComparer">Secondary comparer type. It takes indexes of documents inside batch results.</typeparam>
internal unsafe ref struct NumericalMaxHeapSorter<TTermType, TSecondaryComparer> where TSecondaryComparer : IComparer<int>
    where TTermType : unmanaged
{
    /// <summary>Values are indexes of documents inside batchResult, not the documents themselves.</summary>
    private Span<int> _documents;

    private Span<TTermType> _terms;
    
    private int _heapSize;
    private int _heapCapacity; 
    public bool IsDescending;
    public TSecondaryComparer SecondaryComparer;

    
    /// <summary>
    /// A pointer to compare method.
    /// Params:
    /// this, termA, posA, termB, posB 
    /// </summary>
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

    /// <summary>
    /// Insert new document to the heap
    /// </summary>
    /// <param name="document">Index of document in batchResult</param>
    /// <param name="newTerm">term from document</param>
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
        
        // When documents are equal, we have to stabilize the sort (for paging), so the final order is based on position in batchResults.
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

    /// <summary>
    /// Get sorted results with associated score (when requested) and terms.
    /// Used for spatial sorting.
    /// </summary>
    /// <param name="batchResults">An array with document ids</param>
    /// <param name="results">Destination of sorted results</param>
    /// <param name="terms">Destination of terms</param>
    /// <param name="scoreDestination">Destination of scores (sorted)</param>
    /// <param name="scores">An array with scores associated with batchResults</param>
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
        
        // When we sort a max-heap by repeatedly extracting the maximum value (at index 0), the result is in reverse order (locally - in the heap).
        // Note that we're not dealing with all documents, but only up to heapSize.
        // This structure allows us to make a simple and predictable comparison with the max element 
        // (which is the minimum in the case of descending sorting) to decide if a document
        // should replace the max element in the heap (and then find the new maximum).
        // Reversing the elements is done via Span<T>.Reverse, which is a vectorized operation.
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
