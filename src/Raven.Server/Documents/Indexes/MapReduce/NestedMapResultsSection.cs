using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Paging;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class NestedMapResultsSection
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
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult != null)
            {
                _dataSize = readResult.Reader.Length;
                IsNew = false;
            }
            else
            {
                IsNew = true;
            }
        }

        public bool IsNew { get; }

        public int Size => _dataSize;

        public void Add(long id, BlittableJsonReaderObject result)
        {
            TemporaryPage tmp;
            using (_env.GetTemporaryPage(_parent.Llt, out tmp))
            {
                var dataPosInTempPage = 0;
                var readResult = _parent.Read(_nestedValueKey);
                if (readResult != null)
                {
                    var reader = readResult.Reader;
                    var entry = (ResultHeader*)reader.Base;
                    var end = reader.Base + reader.Length;
                    while (entry < end)
                    {
                        if (entry->Id == id)
                        {
                            if (entry->Size != result.Size)
                            {
                                Delete(id);
                                break;
                            }
                            var dataStart = _parent.DirectAdd(_nestedValueKey, reader.Length);
                            var pos = dataStart + ((byte*)entry - reader.Base + sizeof(ResultHeader));
                            Memory.Copy(pos, result.BasePointer, result.Size);
                            return;// just overwrite it completely
                        }
                        entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
                    }
                    Memory.Copy(tmp.TempPagePointer, readResult.Reader.Base, readResult.Reader.Length);
                    dataPosInTempPage = readResult.Reader.Length;
                }
                Debug.Assert(dataPosInTempPage + sizeof(ResultHeader) + result.Size <= tmp.TempPageBuffer.Length);
                var newEntry = (ResultHeader*)(tmp.TempPagePointer + dataPosInTempPage);
                newEntry->Id = id;
                newEntry->Size = (ushort)result.Size;
                Memory.Copy(tmp.TempPagePointer + dataPosInTempPage + sizeof(ResultHeader), result.BasePointer, result.Size);
                dataPosInTempPage += result.Size + sizeof(ResultHeader);
                var dest = _parent.DirectAdd(_nestedValueKey,dataPosInTempPage);
                Memory.Copy(dest, tmp.TempPagePointer, dataPosInTempPage);
                _dataSize += result.Size + sizeof(ResultHeader);
            }

        }

        public void MoveTo(Tree newHome)
        {
            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                return;

            var reader = readResult.Reader;
            var entry = (ResultHeader*)reader.Base;
            var end = reader.Base + reader.Length;
            long currentId;
            Slice key;
            using(Slice.External(_parent.Llt.Allocator,(byte*)&currentId, sizeof(long),out key))
            while (entry < end)
            {
                currentId = entry->Id;
                var item = newHome.DirectAdd(key,entry->Size);
                Memory.Copy(item, (byte*) entry + sizeof(ResultHeader), entry->Size);
                entry = (ResultHeader*)((byte*)entry + sizeof(ResultHeader) + entry->Size);
            }
            _parent.Delete(_nestedValueKey);
        }

        public int GetResults(JsonOperationContext context,List<BlittableJsonReaderObject> results)
        {

            var readResult = _parent.Read(_nestedValueKey);
            if (readResult == null)
                return 0;

            var reader = readResult.Reader;
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


        public void Delete(long id)
        {
            TemporaryPage tmp;
            using (_env.GetTemporaryPage(_parent.Llt, out tmp))
            {
                var readResult = _parent.Read(_nestedValueKey);
                if (readResult == null)
                    return;
                var reader = readResult.Reader;
                var entry = (ResultHeader*) reader.Base;
                var end = reader.Base + reader.Length;
                while (entry < end)
                {
                    if (entry->Id != id)
                    {
                        entry = (ResultHeader*) ((byte*) entry + sizeof(ResultHeader) + entry->Size);
                        continue;
                    }

                    var copiedDataStart = (byte*) entry - reader.Base;
                    var copiedDataEnd = end - ((byte*) entry + sizeof(ResultHeader) + entry->Size);
                    if (copiedDataEnd == 0 && copiedDataStart == 0)
                    {
                        _parent.Delete(_nestedValueKey);
                        _dataSize = 0;
                        break;
                    }
                    Memory.Copy(tmp.TempPagePointer, reader.Base, copiedDataStart);
                    Memory.Copy(tmp.TempPagePointer + copiedDataStart,
                        reader.Base + copiedDataStart + sizeof(ResultHeader) + entry->Size, copiedDataEnd);

                    var sizeAfterDel = (int)(copiedDataStart + copiedDataEnd);
                    var newVal = _parent.DirectAdd(_nestedValueKey, sizeAfterDel);
                    Memory.Copy(newVal, tmp.TempPagePointer, sizeAfterDel);
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