using System;
using Sparrow.Json;

namespace Sparrow
{
    public unsafe class UnmanagedMemory
    {
        private Memory<byte>? _memory;

        public readonly byte* Address;

        public readonly int Size;

        public Memory<byte> Memory
        {
            get
            {
                if (_memory.HasValue == false)
                {
                    var memoryManager = new UnmanagedMemoryManager(Address, Size);
                    _memory = memoryManager.Memory;
                }

                return _memory.Value;
            }
        }

        public UnmanagedMemory(byte* address, int size)
        {
            Address = address;
            Size = size;
        }
    }
}
