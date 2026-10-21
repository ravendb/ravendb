using System;
using System.IO;
using Sparrow.Json;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal
{
    internal sealed unsafe class SystemTextJsonBlittableReader : IJsonReader
    {
        private AllocatedMemoryData _allocation;
        private JsonOperationContext _context;
        private int _length;

        public void Initialize(BlittableJsonReaderObject blittable)
        {
            _context = blittable._context;

            int estimatedSize = Math.Max(blittable.Size * 2, 512);
            _allocation = _context.GetMemory(estimatedSize);

            while (true)
            {
                using var stream = new UnmanagedMemoryStream(_allocation.Address, 0, _allocation.SizeInBytes, FileAccess.Write);
                try
                {
                    blittable.WriteJsonTo(stream);
                    _length = (int)stream.Position;
                    break;
                }
                catch (NotSupportedException)
                {
                    int needed = _allocation.SizeInBytes * 2;
                    if (_context.GrowAllocation(_allocation, needed - _allocation.SizeInBytes) == false)
                    {
                        var newAllocation = _context.GetMemory(needed);
                        _context.ReturnMemory(_allocation);
                        _allocation = newAllocation;
                    }
                }
            }
        }

        public ReadOnlySpan<byte> GetUtf8Json()
        {
            return new ReadOnlySpan<byte>(_allocation.Address, _length);
        }

        public void ReturnMemory()
        {
            if (_allocation != null && _context != null)
            {
                _context.ReturnMemory(_allocation);
                _allocation = null;
                _context = null;
            }
        }

        public void Dispose()
        {
            ReturnMemory();
        }
    }
}
