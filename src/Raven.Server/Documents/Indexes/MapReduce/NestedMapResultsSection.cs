using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class NestedMapResultsSection : IDisposable
    {
        private static readonly int SizeOfResultHeader = sizeof(ResultHeader);
        
        private readonly TransactionOperationContext _indexContext;
        private readonly List<AllocatedMemoryData> _allocations = new List<AllocatedMemoryData>();
        private readonly Dictionary<long, BlittableJsonReaderObject> _mapResults = new Dictionary<long, BlittableJsonReaderObject>();

        private int _dataSize;
        
        public NestedMapResultsSection(byte* ptr, int size, TransactionOperationContext indexContext)
        {
            _indexContext = indexContext;

            if (size == 0)
                return;

            // need to have a copy because pointer can become invalid after defragmentation of a related page

            var inMemorySection = _indexContext.GetMemory(size);

            _allocations.Add(inMemorySection);

            Memory.Copy((byte*)inMemorySection.Address, ptr, size);

            var readPtr = ptr;

            while (readPtr - ptr < size)
            {
                var resultPtr = (ResultHeader*)readPtr;

                _mapResults.Add(resultPtr->Id, new BlittableJsonReaderObject(readPtr + SizeOfResultHeader, resultPtr->Size, _indexContext));

                readPtr += resultPtr->Size + SizeOfResultHeader;

                _dataSize += resultPtr->Size;
            }
        }

        public NestedMapResultsSection(TransactionOperationContext indexContext)
        {
            _indexContext = indexContext;
        }

        public int Size => _dataSize + _mapResults.Count*SizeOfResultHeader;

        public void Add(long id, BlittableJsonReaderObject result, bool isUpdate)
        {
            var allocation = _indexContext.GetMemory(result.Size);

            _allocations.Add(allocation);

            result.CopyTo((byte*)allocation.Address); // TODO arek - we should be able to use 'result' directly

            if (isUpdate)
                _dataSize -= _mapResults[id].Size;

            _mapResults[id] = new BlittableJsonReaderObject((byte*) allocation.Address, result.Size, _indexContext);

            _dataSize += result.Size;
        }

        public Dictionary<long, BlittableJsonReaderObject> GetResults()
        {
            return _mapResults;
        }

        public void Dispose()
        {

        }

        public void Delete(long id)
        {
            _dataSize -= _mapResults[id].Size;

            _mapResults.Remove(id);
        }

        public void CopyTo(byte* ptr)
        {
            foreach (var result in _mapResults)
            {
                var header = (ResultHeader*)ptr;

                header->Id = result.Key;
                header->Size = (ushort) result.Value.Size;

                ptr += SizeOfResultHeader;

                result.Value.CopyTo(ptr);

                ptr += result.Value.Size;
            }
        }

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct ResultHeader
        {
            [FieldOffset(0)]
            public long Id;

            [FieldOffset(8)]
            public ushort Size;
        }
    }
}