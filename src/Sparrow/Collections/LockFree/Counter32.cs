// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Utils;

namespace Sparrow.Collections.LockFree
{
    [StructLayout(LayoutKind.Sequential)]
    internal sealed class Counter32
    {
        private static readonly int MAX_CELL_COUNT = ProcessorInfo.ProcessorCount * 2;
        private const int MAX_DRIFT = 1;

        private class Cell
        {
            [StructLayout(LayoutKind.Explicit)]
            public struct SpacedCounter
            {
                // 64 bytes - sizeof(int) - sizeof(objecHeader64)
                [FieldOffset(44)]
                public int cnt;
            }

            public SpacedCounter counter;
        }

        // spaced out counters
        private Cell[] cells;

        // default counter
        private int cnt;

        // how many cells we have
        private int cellCount;

        // delayed estimated count
        private int lastCntTicks;
        private int lastCnt;
        
        public Counter32()
        {
        }

        public int Value
        {
            get
            {
                var count = this.cnt;
                var cells = this.cells;

                if (cells != null)
                {
                    for (int i = 0; i < cells.Length; i++)
                    {
                        var cell = cells[i];
                        if (cell != null)
                        {
                            count += cell.counter.cnt;
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                return count;
            }
        }

        public int EstimatedValue
        {
            get
            {
                if (this.cells == null)
                {
                    return this.cnt;
                }

                var curTicks = Environment.TickCount;
                // more than a millisecond passed?
                if (curTicks != lastCntTicks)
                {
                    lastCnt = Value;
                    lastCntTicks = curTicks;
                }

                return lastCnt;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Increment()
        {
            Cell cell = null;

            int curCellCount = this.cellCount;
            if (curCellCount > 1 & this.cells != null)
            {
                cell = this.cells[GetIndex(curCellCount)];
            }

            var drift = cell == null ?
                increment(ref cnt) :
                increment(ref cell.counter.cnt);

            if (drift > MAX_DRIFT)
            {
                TryAddCell(curCellCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(int c)
        {
            Cell cell = null;

            int curCellCount = this.cellCount;
            if (curCellCount > 1 & this.cells != null)
            {
                cell = this.cells[GetIndex(curCellCount)];
            }

            var drift = cell == null ?
                increment(ref cnt, c) :
                increment(ref cell.counter.cnt, c);

            if (drift > MAX_DRIFT)
            {
                TryAddCell(curCellCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Decrement()
        {
            Cell cell = null;

            int curCellCount = this.cellCount;
            if (curCellCount > 1 & this.cells != null)
            {
                cell = this.cells[GetIndex(curCellCount)];
            }

            var drift = cell == null ?
                decrement(ref cnt) :
                decrement(ref cell.counter.cnt);

            if (drift > MAX_DRIFT)
            {
                TryAddCell(curCellCount);
            }
        }

        private static int increment(ref int val)
        {
            return -val + Interlocked.Increment(ref val) - 1;
        }

        private static int increment(ref int val, int inc)
        {
            return -val + Interlocked.Add(ref val, inc) - inc;
        }

        private static int decrement(ref int val)
        {
            return val - Interlocked.Decrement(ref val) - 1;
        }

        private static int GetIndex(int cellCount)
        {
            return Environment.CurrentManagedThreadId % cellCount;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TryAddCell(int curCellCount)
        {
            if (curCellCount < MAX_CELL_COUNT)
            {
                var cells = this.cells;
                if (cells == null)
                {
                    var newCells = new Cell[MAX_CELL_COUNT];
                    cells = Interlocked.CompareExchange(ref this.cells, newCells, null) ?? newCells;
                }

                if (cells[curCellCount] == null)
                {
                    Interlocked.CompareExchange(ref cells[curCellCount], new Cell(), null);
                }

                if (this.cellCount == curCellCount)
                {
                    Interlocked.CompareExchange(ref this.cellCount, curCellCount + 1, curCellCount);
                    //if (Interlocked.CompareExchange(ref this.cellCount, curCellCount + 1, curCellCount) == curCellCount)
                    //{
                    //    System.Console.WriteLine(curCellCount + 1);
                    //}
                }
            }
        }
    }
}
