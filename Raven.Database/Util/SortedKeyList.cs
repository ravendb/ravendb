// -----------------------------------------------------------------------
//  <copyright file="SortedKeyList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Util
{
    /// <summary>
    /// Modified implementation from original at: 
    /// https://github.com/mosa/Mono-Class-Libraries/blob/master/mcs/class/System/System.Collections.Generic/SortedList.cs
    /// </summary>
    public class SortedKeyList<T> : IEnumerable<T>
        where T : class
    {
        private const int InitialSize = 16;
        private readonly IComparer<T> comparer;
        private int inUse;
        private int modificationCount;
        private T[] table;

        public SortedKeyList()
            : this(Comparer<T>.Default)
        {
        }

        public SortedKeyList(IComparer<T> comparer)
        {
            this.comparer = comparer;
            table = new T[InitialSize];
        }

        public int Count
        {
            get { return inUse; }
        }

        public int Capacity
        {
            get { return table.Length; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            int startModificationCount = modificationCount;
            for (int i = 0; i < inUse; i++)
            {
                if (startModificationCount != modificationCount)
                    throw new InvalidOperationException("Enumeration target was modified");
                yield return table[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            if (item == null)
                throw new ArgumentNullException("item");

            int freeIndx = Find(item);

            if (freeIndx >= 0)
            {
                table[freeIndx] = item;
                ++modificationCount;
                return;
            }

            freeIndx = ~freeIndx;

            if (freeIndx > Capacity + 1)
                throw new Exception("SortedKeyList::internal error (" + item + ") at [" + freeIndx + "]");


            EnsureCapacity(Count + 1, freeIndx);

            table[freeIndx] = item;

            ++inUse;
            ++modificationCount;
        }

        public bool Contains(T item)
        {
            return Find(item) >= 0;
        }

        public void RemoveSmallerOrEqual(T item)
        {
            if (item == null)
                throw new ArgumentNullException("item");

            int index = FindSmallerOfEqual(item);

            if (index <= 0)
                return;
            int toRemove = index + 1;

            Array.Copy(table, toRemove, table, 0, inUse - toRemove);
            inUse -= toRemove;
            Array.Clear(table,inUse, table.Length - inUse);
        }

        private int FindSmallerOfEqual(T key)
        {
            int len = Count;

            if (len == 0) return ~0;

            int left = 0;
            int right = len - 1;

            while (left <= right)
            {
                int guess = (left + right) >> 1;

                int cmp = comparer.Compare(table[guess], key);
                if (cmp == 0) return guess;

                if (cmp < 0) left = guess + 1;
                else right = guess - 1;
            }

            return Math.Min(left, right);
        }

        private void EnsureCapacity(int n, int free)
        {
            int cap = Capacity;
            bool gap = (free >= 0 && free < Count);

            if (n > cap)
            {
                var newTable = new T[n << 1];
                if (gap)
                {
                    int copyLen = free;
                    if (copyLen > 0)
                    {
                        Array.Copy(table, 0, newTable, 0, copyLen);
                    }
                    copyLen = Count - free;
                    if (copyLen > 0)
                    {
                        Array.Copy(table, free, newTable, free + 1, copyLen);
                    }
                }
                else
                {
                    // Just a resizing, copy the entire table.
                    Array.Copy(table, newTable, Count);
                }
                table = newTable;
            }
            else if (gap)
            {
                Array.Copy(table, free, table, free + 1, Count - free);
            }
        }

        private int Find(T key)
        {
            int len = Count;

            if (len == 0) return ~0;

            int left = 0;
            int right = len - 1;

            while (left <= right)
            {
                int guess = (left + right) >> 1;

                int cmp = comparer.Compare(table[guess], key);
                if (cmp == 0) return guess;

                if (cmp < 0) left = guess + 1;
                else right = guess - 1;
            }

            return ~left;
        }
    }
}