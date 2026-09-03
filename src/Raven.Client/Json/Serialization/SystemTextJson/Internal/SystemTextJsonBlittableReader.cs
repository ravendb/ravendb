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

            // Blittable binary is more compact than UTF-8 JSON text.
            // Allocate 2x blittable size from context native memory as initial estimate.
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
                    // Buffer too small - keep doubling until WriteJsonTo succeeds
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

        /// <summary>
        /// Return the native memory to the context immediately.
        /// Call this as soon as deserialization is complete, while still in the same context scope.
        /// </summary>
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
            // Reader is cached in LightWeightThreadLocal and outlives the session/context.
            // Don't return memory here - use ReturnMemory() explicitly after deserialization.
        }
    }
}
