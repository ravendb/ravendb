using System;
using System.Collections.Concurrent;
using Sparrow;
using Sparrow.Binary;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl
{
    public unsafe class EncryptionBuffersPool : IDisposable, ILowMemoryHandler
    {
        private class NativeAllocation
        {
            public IntPtr Ptr;
            public int Size;
            public NativeMemory.ThreadStats AllocatingThread;
        }

        private readonly ConcurrentStack<NativeAllocation>[] _items;
        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

        public EncryptionBuffersPool()
        {
            _items = new ConcurrentStack<NativeAllocation>[32];
            for (int i = 0; i < _items.Length; i++)
            {
                _items[i] = new ConcurrentStack<NativeAllocation>();
            }

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(() =>
            {
                ReleaseUnmanagedResources();
                GC.SuppressFinalize(this);
            });
        }

        public byte* Get(int size, out NativeMemory.ThreadStats thread)
        {
            size = Bits.PowerOf2(size);

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

            size = Bits.PowerOf2(size);
            Sodium.sodium_memzero(ptr, (UIntPtr)size);

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
            _disposeOnceRunner.Dispose();
        }

        ~EncryptionBuffersPool()
        {
            try
            {
                Dispose();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void LowMemory()
        {
            ReleaseUnmanagedResources();
        }

        public void LowMemoryOver()
        {
        }
    }
}
