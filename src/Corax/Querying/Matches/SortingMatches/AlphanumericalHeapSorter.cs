using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using Voron.Util;

namespace Corax.Querying.Matches.SortingMatches;

internal ref struct AlphanumericalHeapSorter
{
    private Span<int> _documents;
    private Span<ByteString> _terms;
    private int _heapSize;
    private int _heapCapacity;
    private ByteStringContext _allocator;
    private bool _descending;

    public void Init(Span<int> documents, Span<ByteString> terms, ByteStringContext allocator, bool descending)
    {
        _descending = descending;
        _allocator = allocator;
        _documents = documents;
        _terms = terms;
        _heapCapacity = documents.Length;
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

            // We guarantee that we'll have at least _heapCapacity items, so let's build the heap only when we have the full array.
            if (_heapCapacity == _heapSize)
                BuildMaxHeap();

            return;
        }

        // Since we're gathering the smallest N keys, we can do a simple comparison cmp(Max_Heap, New),
        // as every new item will have to be smaller than the maximum in the heap.
        var isNewTermSmallerThanCurrentMax = AlphanumericalComparer.Instance.Compare(_terms[0].ToSpan(), newTerm, _descending);
        if (isNewTermSmallerThanCurrentMax > 0)
        {
            _allocator.Allocate(newTerm.Length, out ByteString mem);
            newTerm.CopyTo(mem.ToSpan());

            ReplaceMax(mem, document);
            ValidateMaxHeapStructure();
        }
    }

    private void BuildMaxHeap()
    {
        for (int i = (_heapSize / 2) - 1; i >= 0; --i)
            MaxHeapify(i);

        ValidateMaxHeapStructure();
    }

    private void MaxHeapify(int i)
    {
#if DEBUG
        RuntimeHelpers.EnsureSufficientExecutionStack();
#endif

        int largest;
        int leftChild = LeftChild(i);
        int rightChild = RightChild(i);

        // left child > current
        if (leftChild < _heapSize && AlphanumericalComparer.Instance.Compare(_terms[leftChild].ToSpan(), _terms[i].ToSpan(), _descending) > 0)
            largest = leftChild;
        else
            largest = i;

        // right child > largest
        if (rightChild < _heapSize && AlphanumericalComparer.Instance.Compare(_terms[rightChild].ToSpan(), _terms[largest].ToSpan(), _descending) > 0)
            largest = rightChild;

        if (largest != i)
        {
            Swap(largest, i);
            MaxHeapify(largest);
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
        ValidateMaxHeapStructure();
    }

    public void Fill(Span<long> batchResults, ref ContextBoundNativeList<long> results)
    {
        var start = results.Count;
        results.EnsureCapacityFor(_heapSize);
        int documentsToReturn = _heapSize;
        while (documentsToReturn > 0)
        {
            results.AddUnsafe(batchResults[_documents[0]]);
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
    private int LeftChild(int idX) => idX * 2 + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int RightChild(int idX) => idX * 2 + 2;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Swap(int a, int b)
    {
        (_documents[a], _documents[b]) = (_documents[b], _documents[a]);
        (_terms[a], _terms[b]) = (_terms[b], _terms[a]);
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
                if (AlphanumericalComparer.Instance.Compare(currentTerm, _terms[leftChild].ToSpan(), _descending) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(_descending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(leftChild);
            }

            if (rightChild < _heapSize)
            {
                if (AlphanumericalComparer.Instance.Compare(currentTerm, _terms[rightChild].ToSpan(), _descending) < 0)
                    throw new InvalidDataException($"Heap is corrupted.: `{_terms[node]}` {(_descending ? ">" : "<")} `{_terms[leftChild]}`");

                queue.Enqueue(rightChild);
            }
        }
    }
}
