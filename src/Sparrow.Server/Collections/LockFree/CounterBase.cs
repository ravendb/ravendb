// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Collections.LockFree
{
    /// <summary>
    /// Scalable counter base.
    /// </summary>
    public class CounterBase
    {
        private protected const int CACHE_LINE = 64;
        private protected const int OBJ_HEADER_SIZE = 8;

        private protected static readonly int MAX_CELL_COUNT = Environment.ProcessorCount;

        // how many cells we have
        private protected int cellCount;

        // delayed count time
        private protected uint lastCntTicks;

        private protected CounterBase()
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private protected static int GetIndex(int cellCount)
        {
            return Environment.CurrentManagedThreadId % cellCount;
        }
    }
}
