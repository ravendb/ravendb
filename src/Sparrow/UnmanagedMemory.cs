using System;
using Sparrow.Json;

namespace Sparrow
{
    public sealed unsafe class UnmanagedMemory(byte* address, int size)
    {
        private Memory<byte>? _memory;

        public readonly byte* Address = address;
        public readonly int Size = size;

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
    }
}
