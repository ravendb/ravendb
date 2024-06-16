using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Lucene.Net.Util
{
    public struct Sorter<T, TSorter>
        where TSorter : struct, IComparer<T>
    {
        private readonly TSorter _sorter;

        private const int SizeThreshold = 7;

        public Sorter(TSorter sorter)
        {
            this._sorter = sorter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(T[] keys)
        {
            if (keys.Length < 2)
                return;

            IntroSort(keys, 0, keys.Length - 1, 2 * BitUtil.FloorLog2(keys.Length));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(T[] keys, int index, int length)
        {
            Debug.Assert(keys != null, "Check the arguments in the caller!");
            Debug.Assert(index >= 0 && length >= 0 && (keys.Length - index >= length), "Check the arguments in the caller!");
            if (length < 2)
                return;

            IntroSort(keys, index, length + index - 1, 2 * BitUtil.FloorLog2(keys.Length));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SwapIfGreaterWithItems3(T[] keys, int a, int b, int c)
        {
            int x = a;
            int y = b == c ? b : c;
            int times = b == c ? 3 : -1;

            CompareLoop:
            bool swapped = false;
            times++;

            ref T ka = ref keys[x];
            ref T kb = ref keys[y];
            if (ka != null && _sorter.Compare(ka, kb) > 0)
            {
                T aux = ka;
                ka = kb;
                kb = aux;
                swapped = true;
            }

            if (times == 0)
            {
                y = b;
                goto CompareLoop;
            }
            if (times == 1 && !swapped)
            {
                x = b;
                y = c;
                goto CompareLoop;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SwapIfGreaterWithItems(T[] keys, int a, int b)
        {
            ref T ka = ref keys[a];
            ref T kb = ref keys[b];

            if (ka != null && _sorter.Compare(ka, kb) > 0)
            {
                T aux = ka;
                ka = kb;
                kb = aux;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TrySwapIfGreaterWithItems(T[] keys, int a, int b)
        {
            ref T ka = ref keys[a];
            ref T kb = ref keys[b];

            bool swapped = false;
            if (ka != null && _sorter.Compare(ka, kb) > 0)
            {
                T aux = ka;
                ka = kb;
                kb = aux;
                swapped = true;
            }

            return swapped;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Swap(T[] keys, int x, int y)
        {
            ref T kx = ref keys[x];
            ref T ky = ref keys[y];

            T aux = kx;
            kx = ky;
            ky = aux;
        }

        private void IntroSort(T[] keys, int lo, int hi, int depthLimit)
        {
            int partitionSize;
            while (hi > lo)
            {
                partitionSize = hi - lo + 1;
                if (partitionSize <= SizeThreshold)
                    goto InsertionSortEnd;

                if (depthLimit == 0)
                    goto HeapSortEnd;

                depthLimit--;

                // Compute median-of-three.  But also partition them, since we've done the comparison.
                int middle = (hi + lo) / 2;

                // Sort lo, mid and hi appropriately, then pick mid as the pivot.
                SwapIfGreaterWithItems3(keys, lo, middle, hi);

                T pivot = keys[middle];
                int p;
                if (pivot == null) // TODO: When we know that T is a struct use conditional compilation to avoid the check (CoreCLR 2.0)
                {
                    p = PickPivotAndPartitionUnlikely(keys, lo, hi, middle);
                    goto PickPivotUnlikely;
                }

                Swap(keys, middle, hi - 1);

                int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
                while (left < right)
                {
                    while (_sorter.Compare(pivot, keys[++left]) > 0) ;
                    while (_sorter.Compare(pivot, keys[--right]) < 0) ;

                    if (left >= right)
                        break;

                    Swap(keys, left, right);
                }

                // Put pivot in the right location.
                Swap(keys, left, hi - 1);

                p = left;

                PickPivotUnlikely:

                // Note we've already partitioned around the pivot and do not have to move the pivot again.
                IntroSort(keys, p + 1, hi, depthLimit);
                hi = p - 1;
            }

            return;

            InsertionSortEnd:
            {
                if (partitionSize >= 4) // Probabilistically the most likely case
                {
                    InsertionSort(keys, lo, hi);
                    goto Return; // We are done with this segment.
                }

                if (partitionSize >= 2)
                {
                    int mid = partitionSize == 3 ? hi - 1 : hi;
                    SwapIfGreaterWithItems3(keys, lo, mid, hi);
                }

                goto Return; // We are done with this segment.
            }

            HeapSortEnd:
            {
                HeapSort(keys, lo, hi);
            }

            Return:
            ;
        }

        private int PickPivotAndPartitionUnlikely(T[] keys, int lo, int hi, int middle)
        {
            Swap(keys, middle, hi - 1);

            int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
            while (left < right)
            {
                while (left < (hi - 1) && keys[++left] == null) ;
                while (right > lo && keys[--right] != null) ;

                if (left >= right)
                    break;

                Swap(keys, left, right);
            }

            // Put pivot in the right location.
            Swap(keys, left, hi - 1);

            return left;
        }

        private void HeapSort(T[] keys, int lo, int hi)
        {
            int n = hi - lo + 1;
            for (int i = n / 2; i >= 1; i--)
            {
                DownHeap(keys, i, n, lo);
            }
            for (int i = n; i > 1; i--)
            {
                Swap(keys, lo, lo + i - 1);
                DownHeap(keys, 1, i - 1, lo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DownHeap(T[] keys, int i, int n, int lo)
        {
            T d = keys[lo + i - 1];
            while (i <= n / 2)
            {
                int child = 2 * i;
                ref var key = ref keys[lo + child - 1];
                if (child < n && (key == null || _sorter.Compare(key, keys[lo + child]) < 0))
                {
                    child++;
                }

                ref var key2 = ref keys[lo + child - 1];
                if (key2 == null || _sorter.Compare(key2, d) < 0)
                    break;

                keys[lo + i - 1] = key2;
                i = child;
            }
            keys[lo + i - 1] = d;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InsertionSort(T[] keys, int lo, int hi)
        {
            for (int i = lo; i < hi; i++)
            {
                int j = i;
                T t = keys[i + 1];

                while (j >= lo && (t == null || _sorter.Compare(t, keys[j]) < 0))
                {
                    keys[j + 1] = keys[j];
                    j--;
                }
                keys[j + 1] = t;
            }
        }
    }

    public struct Sorter<T, V, TSorter>
        where TSorter : struct, IComparer<T>
    {
        private readonly TSorter _sorter;

        private const int SizeThreshold = 16;

        public Sorter(TSorter sorter)
        {
            this._sorter = sorter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Sort(T[] keys, V[] values)
        {
            if (keys.Length < 2)
                return;

            IntroSort(keys, values, 0, keys.Length - 1, 2 * BitUtil.FloorLog2(keys.Length));
        }

        public void Sort(T[] keys, V[] values, int index, int length)
        {
            Debug.Assert(keys != null, "Check the arguments in the caller!");
            Debug.Assert(index >= 0 && length >= 0 && (keys.Length - index >= length), "Check the arguments in the caller!");
            if (length < 2)
                return;

            IntroSort(keys, values, index, length + index - 1, 2 * BitUtil.FloorLog2(keys.Length));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SwapIfGreaterWithItems3(T[] keys, V[] values, int a, int b, int c)
        {
            int x = a;
            int y = b == c ? b : c;
            int times = b == c ? 3 : -1;

            CompareLoop:
            bool swapped = false;
            times++;

            ref T ka = ref keys[x];
            ref T kb = ref keys[y];
            if (ka != null && _sorter.Compare(ka, kb) > 0)
            {
                ref V va = ref values[x];
                ref V vb = ref values[y];

                T aux = ka;
                V auxV = va;

                ka = kb;
                va = vb;

                kb = aux;
                vb = auxV;

                swapped = true;
            }

            if (times == 0)
            {
                y = b;
                goto CompareLoop;
            }
            if (times == 1 && !swapped)
            {
                x = b;
                y = c;
                goto CompareLoop;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Swap(T[] keys, V[] values, int x, int y)
        {
            T aux = keys[x];
            V vAux = values[x];

            keys[x] = keys[y];
            values[x] = values[y];

            keys[y] = aux;
            values[y] = vAux;
        }

        private void IntroSort(T[] keys, V[] values, int lo, int hi, int depthLimit)
        {
            int partitionSize;
            while (hi > lo)
            {
                partitionSize = hi - lo + 1;
                if (partitionSize <= SizeThreshold)
                    goto InsertionSortEnd;

                if (depthLimit == 0)
                    goto HeapSortEnd;

                depthLimit--;

                // Compute median-of-three.  But also partition them, since we've done the comparison.
                int middle = (hi + lo) / 2;

                // Sort lo, mid and hi appropriately, then pick mid as the pivot.
                SwapIfGreaterWithItems3(keys, values, lo, middle, hi);

                T pivot = keys[middle];
                int p;
                if (pivot == null) // TODO: When we know that T is a struct use conditional compilation to avoid the check (CoreCLR 2.0)
                {
                    p = PickPivotAndPartitionUnlikely(keys, values, lo, hi, middle);
                    goto PickPivotUnlikely;
                }

                Swap(keys, values, middle, hi - 1);

                int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
                while (left < right)
                {
                    while (_sorter.Compare(pivot, keys[++left]) > 0) ;
                    while (_sorter.Compare(pivot, keys[--right]) < 0) ;

                    if (left >= right)
                        break;

                    Swap(keys, values, left, right);
                }

                // Put pivot in the right location.
                Swap(keys, values, left, hi - 1);

                p = left;

                PickPivotUnlikely:

                // Note we've already partitioned around the pivot and do not have to move the pivot again.
                IntroSort(keys, values, p + 1, hi, depthLimit);
                hi = p - 1;
            }

            return;

            InsertionSortEnd:
            {
                if (partitionSize >= 4) // Probabilistically the most likely case
                {
                    InsertionSort(keys, values, lo, hi);
                    goto Return; // We are done with this segment.
                }

                if (partitionSize >= 2)
                {
                    int mid = partitionSize == 3 ? hi - 1 : hi;
                    SwapIfGreaterWithItems3(keys, values, lo, mid, hi);
                }

                goto Return; // We are done with this segment.
            }

            HeapSortEnd:
            {
                HeapSort(keys, values, lo, hi);
            }

            Return:;
        }

        private int PickPivotAndPartitionUnlikely(T[] keys, V[] values, int lo, int hi, int middle)
        {
            Swap(keys, values, middle, hi - 1);

            int left = lo, right = hi - 1; // We already partitioned lo and hi and put the pivot in hi - 1.  And we pre-increment & decrement below.
            while (left < right)
            {
                while (left < (hi - 1) && keys[++left] == null) ;
                while (right > lo && keys[--right] != null) ;

                if (left >= right)
                    break;

                Swap(keys, values, left, right);
            }

            // Put pivot in the right location.
            Swap(keys, values, left, hi - 1);

            return left;
        }

        private void HeapSort(T[] keys, V[] values, int lo, int hi)
        {
            int n = hi - lo + 1;
            for (int i = n / 2; i >= 1; i--)
            {
                DownHeap(keys, values, i, n, lo);
            }
            for (int i = n; i > 1; i--)
            {
                Swap(keys, values, lo, lo + i - 1);
                DownHeap(keys, values, 1, i - 1, lo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DownHeap(T[] keys, V[] values, int i, int n, int lo)
        {
            T d = keys[lo + i - 1];
            V v = values[lo + i - 1];
            while (i <= n / 2)
            {
                int child = 2 * i;
                ref var key = ref keys[lo + child - 1];
                if (child < n && (key == null || _sorter.Compare(key, keys[lo + child]) < 0))
                {
                    child++;
                }

                ref var key2 = ref keys[lo + child - 1];
                if (key2 == null || _sorter.Compare(key2, d) < 0)
                    break;

                keys[lo + i - 1] = key2;
                values[lo + i - 1] = values[lo + child - 1];

                i = child;
            }

            keys[lo + i - 1] = d;
            values[lo + i - 1] = v;
        }

        private void InsertionSort(T[] keys, V[] values, int lo, int hi)
        {
            int i, j;
            T t;
            V v;
            for (i = lo; i < hi; i++)
            {
                j = i;
                t = keys[i + 1];
                v = values[i + 1];
                while (j >= lo && (t == null || _sorter.Compare(t, keys[j]) < 0))
                {
                    keys[j + 1] = keys[j];
                    values[j + 1] = values[j];
                    j--;
                }
                keys[j + 1] = t;
                values[j + 1] = v;
            }
        }
    }
}
