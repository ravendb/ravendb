using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : IDisposable
    {
        private readonly ConcurrentStack<IntPtr>[] _items;
        private bool _isDisposed;

        public EncryptionBuffersPool()
        {
            _items = new ConcurrentStack<IntPtr>[32];
            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new ConcurrentStack<IntPtr>();
            }
        }

        public byte* Get(int size)
        {
            size = Bits.NextPowerOf2(size);

            if (size > Constants.Size.Megabyte * 16)
            {
                // We don't want to pool large buffers
                return UnmanagedMemory.Allocate4KbAlignedMemory(size);
            }

            var index = Bits.MostSignificantBit(size);

            if (_items[index].TryPop(out var buffer))
            {
                return (byte*)buffer;
            }

            return UnmanagedMemory.Allocate4KbAlignedMemory(size);
        }

        public void Return(byte* ptr, int size)
        {
            if (ptr == null)
                return;

            size = Bits.NextPowerOf2(size);
            Sodium.ZeroMemory(ptr, size);

            if (size > Constants.Size.Megabyte * 16)
            {
                // We don't want to pool large buffers
                UnmanagedMemory.Free(ptr);
                return;
            }

            var index = Bits.MostSignificantBit(size);
            _items[index].Push((IntPtr)ptr);
        }

        private void ReleaseUnmanagedResources()
        {
            foreach (var stack in _items)
            {
                while (stack.TryPop(out var bufferPtr))
                {
                    UnmanagedMemory.Free((byte*)bufferPtr);
                }
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            ReleaseUnmanagedResources();

            _isDisposed = true;

            GC.SuppressFinalize(this);
        }

        ~EncryptionBuffersPool()
        {
            Dispose();
        }
    }
}
