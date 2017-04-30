using System;
using System.Collections.Concurrent;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : IDisposable
    {
        private class NativeAllocation
        {
            public IntPtr Ptr;
            public int Size;
            public NativeMemory.ThreadStats AllocatingThread;
        }

        private readonly ConcurrentStack<NativeAllocation>[] _items;
        private bool _isDisposed;

        public EncryptionBuffersPool()
        {
            _items = new ConcurrentStack<NativeAllocation>[32];
            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new ConcurrentStack<NativeAllocation>();
            }
        }

        public byte* Get(int size, out NativeMemory.ThreadStats thread)
        {
            size = Bits.NextPowerOf2(size);

            if (size > Constants.Size.Megabyte * 16)
            {
                // We don't want to pool large buffers
                return NativeMemory.Allocate4KbAlignedMemory(size, out thread);
            }

            var index = Bits.MostSignificantBit(size);

            if (_items[index].TryPop(out var allocation))
            {
                thread = allocation.AllocatingThread;
                return (byte*)allocation.Ptr;
            }

            return NativeMemory.Allocate4KbAlignedMemory(size, out thread);
        }

        public void Return(byte* ptr, int size, NativeMemory.ThreadStats allocatingThread)
        {
            if (ptr == null)
                return;

            size = Bits.NextPowerOf2(size);
            Sodium.ZeroMemory(ptr, size);

            if (size > Constants.Size.Megabyte * 16)
            {
                // We don't want to pool large buffers
                NativeMemory.Free4KbAlignedMemory(ptr, size, allocatingThread);
                return;
            }

            var index = Bits.MostSignificantBit(size);
            _items[index].Push(new NativeAllocation()
            {
                Ptr = (IntPtr)ptr,
                AllocatingThread = allocatingThread,
                Size = size
            });
        }

        private void ReleaseUnmanagedResources()
        {
            foreach (var stack in _items)
            {
                while (stack.TryPop(out var allocation))
                {
                    NativeMemory.Free4KbAlignedMemory((byte*)allocation.Ptr, allocation.Size, allocation.AllocatingThread);
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
