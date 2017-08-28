// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Collections.LockFree
{
    internal sealed class DictionaryImplInt<TValue> : DictionaryImpl<int, int, TValue>
    {
        internal DictionaryImplInt(int capacity, ConcurrentDictionary<int, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplInt(int capacity, DictionaryImplInt<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref int entryKey, int key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref int entryKey, int key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref int entryKey, int key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<int, int, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplInt<TValue>(capacity, this);
        }
    }

    internal sealed class DictionaryImplIntNoComparer<TValue>
            : DictionaryImpl<int, int, TValue>
    {
        internal DictionaryImplIntNoComparer(int capacity, ConcurrentDictionary<int, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplIntNoComparer(int capacity, DictionaryImplIntNoComparer<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref int entryKey, int key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref int entryKey, int key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryClaimSlot(ref int entryKey, int key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue;
        }

        protected override DictionaryImpl<int, int, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplIntNoComparer<TValue>(capacity, this);
        }
    }
}
