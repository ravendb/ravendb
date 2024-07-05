using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using System;
using Sparrow.Server;
using Voron.Util;
using Voron.Global;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public sealed unsafe class NestedMapResultsSection
    {
        private readonly StorageEnvironment _env;
        private readonly Tree _parent;
        private readonly Slice _nestedValueKey;

        private int _dataSize;

        public NestedMapResultsSection(StorageEnvironment env,Tree parent, Slice nestedValueKey)
        {
            _env = env;
            _parent = parent;
            _nestedValueKey = nestedValueKey;

            if (_parent.TryRead(_nestedValueKey, out var reader))
            {
                _dataSize = reader.Length;
                IsNew = false;
            }
            else
            {
                IsNew = true;
            }
        }

        public bool IsNew { get; }

        public int Size => _dataSize;

        public bool IsModified { get; private set; }

        public Slice Name => _nestedValueKey;

        public TreePage RelevantPage => _parent.FindPageFor(_nestedValueKey, out TreeNodeHeader* _);

        public void Add(long id, BlittableJsonReaderObject result)
        {
            IsModified = true;

            using (_parent.Llt.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp))
            {
                tmp.Clear();
                var tmpPtr = tmp.Ptr;
                var dataPosInTempPage = 0;

                bool isValidReader = _parent.TryRead(_nestedValueKey, out var reader);
                if (isValidReader)
                {
                    var entry = (ResultHeader*)reader.Base;
                    var end = reader.Base + reader.Length;
                    while (entry < end)
                    {
                        if (entry->Id == id)
                        {
                            if (entry->Size != result.Size)
                            {
                                Delete(id);

                                isValidReader = _parent.TryRead(_nestedValueKey, out reader);
                                break;
                            }
                            using (_parent.DirectAdd(_nestedValueKey, reader.Length, out byte* ptr))
                            {
                                var pos = ptr + ((byte*)entry - reader.Base + sizeof(ResultHeader));
                                Memory.Copy(pos, result.BasePointer, result.Size);
                                return;// just overwrite it completely
                            }
                        }
                        entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
                    }

                    if (isValidReader)
                    {
                        Memory.Copy(tmpPtr, reader.Base, reader.Length);
                        dataPosInTempPage = reader.Length;
                    }
                }

                Debug.Assert(dataPosInTempPage + sizeof(ResultHeader) + result.Size <= tmp.Length);
                var newEntry = (ResultHeader*)(tmpPtr + dataPosInTempPage);
                newEntry->Id = id;
                newEntry->Size = (ushort)result.Size;
                Memory.Copy(tmpPtr + dataPosInTempPage + sizeof(ResultHeader), result.BasePointer, result.Size);
                dataPosInTempPage += result.Size + sizeof(ResultHeader);
                using (_parent.DirectAdd(_nestedValueKey, dataPosInTempPage, out byte* destPtr))
                    Memory.Copy(destPtr, tmpPtr, dataPosInTempPage);

                _dataSize += result.Size + sizeof(ResultHeader);
            }

        }
        public PtrSize Get(long id)
        {
            if (_parent.TryRead(_nestedValueKey, out var reader) == false)
                throw new InvalidOperationException($"Could not find a map result wit id '{id}' within a nested values section stored under '{_nestedValueKey}' key");

            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            while (entry < end)
            {
                if (entry->Id == id)
                {
                    return PtrSize.Create((byte*)entry + sizeof(ResultHeader), entry->Size);
                }

                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }

            throw new InvalidOperationException($"Could not find a map result wit id '{id}' within a nested values section stored under '{_nestedValueKey}' key");
        }

        public void MoveTo(Tree newHome)
        {
            if (_parent.TryRead(_nestedValueKey, out var reader) == false)
                return;

            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            long currentId;
            using (Slice.External(_parent.Llt.Allocator, (byte*)&currentId, sizeof(long), out Slice key))
            {
                while (entry < end)
                {
                    currentId = entry->Id;

                    using (newHome.DirectAdd(key, entry->Size, out byte* ptr))
                        Memory.Copy(ptr, (byte*)entry + sizeof(ResultHeader), entry->Size);

                    entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
                }
            }

            _parent.Delete(_nestedValueKey);
        }

        public int GetResults(JsonOperationContext context, List<BlittableJsonReaderObject> results)
        {
            if (_parent.TryRead(_nestedValueKey, out var reader) == false)
                return 0;

            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            int entries = 0;
            while (entry < end)
            {
                entries++;
                results.Add(new BlittableJsonReaderObject((byte*) entry + sizeof(ResultHeader), entry->Size, context));
                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            return entries;
        }

        public int GetResultsForDebug(JsonOperationContext context, Dictionary<long, BlittableJsonReaderObject> results)
        {
            if (_parent.TryRead(_nestedValueKey, out var reader) == false)
                return 0;

            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            int entries = 0;
            while (entry < end)
            {
                entries++;
                results.Add(entry->Id, new BlittableJsonReaderObject((byte*)entry + sizeof(ResultHeader), entry->Size, context));
                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            return entries;
        }

        public void Delete(long id)
        {
            IsModified = true;

            using(_parent.Llt.Allocator.Allocate(Constants.Storage.PageSize, out ByteString tmp))
            {
                tmp.Clear();
                var tmpPtr = tmp.Ptr;

                if (_parent.TryRead(_nestedValueKey, out var reader) == false)
                    return;
                
                var entry = (ResultHeader*)reader.Base;
                var end = reader.Base + reader.Length;
                while (entry < end)
                {
                    if (entry->Id != id)
                    {
                        entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
                        continue;
                    }

                    var copiedDataStart = (byte*)entry - reader.Base;
                    var copiedDataEnd = end - ((byte*)entry + sizeof(ResultHeader) + entry->Size);
                    if (copiedDataEnd == 0 && copiedDataStart == 0)
                    {
                        _parent.Delete(_nestedValueKey);
                        _dataSize = 0;
                        break;
                    }
                    Memory.Copy(tmpPtr, reader.Base, copiedDataStart);
                    Memory.Copy(tmpPtr + copiedDataStart,
                        reader.Base + copiedDataStart + sizeof(ResultHeader) + entry->Size, copiedDataEnd);

                    var sizeAfterDel = (int)(copiedDataStart + copiedDataEnd);
                    using (_parent.DirectAdd(_nestedValueKey, sizeAfterDel, out byte* ptr))
                        Memory.Copy(ptr, tmpPtr, sizeAfterDel);

                    _dataSize -= reader.Length - sizeAfterDel;
                    break;
                }
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

        public int SizeAfterAdding(BlittableJsonReaderObject item)
        {
            return _dataSize + item.Size + sizeof(ResultHeader);
        }
    }
}
