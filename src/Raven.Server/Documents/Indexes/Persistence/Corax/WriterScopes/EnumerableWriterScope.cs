using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Queries;
using Corax.Utils;
using JetBrains.Annotations;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes
{
    public unsafe class EnumerableWriterScope : IWriterScope
    {
        //todo maciej: this is only temp implementation. Related: https://issues.hibernatingrhinos.com/issue/RavenDB-17243
        private readonly ByteStringContext _allocator;
        private bool _isDynamic = false;
        private readonly List<ByteString> _stringValues;
        private readonly List<long> _longValues;
        private readonly List<double> _doubleValues;
        private readonly List<CoraxSpatialPointEntry> _spatialValues;

        [CanBeNull]
        private string _persistedName;

        private int? _persistedId;
        
        private bool _hasNulls;
        private readonly List<BlittableJsonReaderObject> _blittableJsonReaderObjects;
        private (int Strings, int Longs, int Doubles, int Raws, int Spatials) _count;
        
        /// <summary>
        /// This method is only for dynamic fields usage. In other case there is no need to flush.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="fieldId"></param>
        /// <param name="entryWriter"></param>
        private void FlushWhenNecessary(string path, int fieldId, ref IndexEntryWriter entryWriter)
        {
            _persistedName ??= path;
            _persistedId ??= fieldId;
            
            if (path != _persistedName || fieldId != _persistedId)
            {
                Finish(_persistedName, _persistedId.Value, ref entryWriter);
                _persistedName = path;

                _persistedId = fieldId;
                _persistedName = path;
            }
        }
        
        public EnumerableWriterScope(List<ByteString> stringValues, List<long> longValues, List<double> doubleValues, List<CoraxSpatialPointEntry> spatialValues,
            List<BlittableJsonReaderObject> blittableJsonReaderObjects, ByteStringContext allocator)
        {
            _count = (0, 0, 0, 0, 0);
            _doubleValues = doubleValues;
            _longValues = longValues;
            _stringValues = stringValues;
            _spatialValues = spatialValues;
            _blittableJsonReaderObjects = blittableJsonReaderObjects;
            _allocator = allocator;
        }

        public void WriteNull(string path, int field, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);

            // We cannot know if we are writing a tuple or a list. But we know that at finish
            // we will be able to figure out based on the stored counts. Therefore,
            // we will write a null here and then write the real value in the finish method.
            _stringValues.Add(default);
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
            _count.Strings++;
            _hasNulls = true;
        }

        public void Write(string path, int field, ReadOnlySpan<byte> value, ref IndexEntryWriter entryWriter)
        {
            if (_count.Longs != 0 || _count.Doubles != 0)
                throw new InvalidOperationException("Cannot mix tuples writes with straightforward writes");

            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            // Copy the value to write into memory allocated and controlled by the scope.  
            _allocator.Allocate(value.Length, out var buffer);
            value.CopyTo(buffer.ToSpan());

            _count.Strings++;
            _stringValues.Add(buffer);
            _longValues.Add(0);
            _doubleValues.Add(float.NaN);
        }

        public void Write(string path, int field, ReadOnlySpan<byte> value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            if (_count.Strings != _count.Longs || _count.Strings != _count.Doubles)
                throw new InvalidOperationException("Cannot write a tuple with a different number of values than the previous tuple.");

            // Copy the value to write into memory allocated and controlled by the scope.  
            _allocator.Allocate(value.Length, out var buffer);
            value.CopyTo(buffer.ToSpan());

            _stringValues.Add(buffer);
            _longValues.Add(longValue);
            _doubleValues.Add(doubleValue);
            _count.Strings++;
            _count.Longs++;
            _count.Doubles++;
        }

        public void Write(string path, int field, string value, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            _allocator.Allocate(Encoding.UTF8.GetMaxByteCount(value.Length), out var buffer);

            var length = Encoding.UTF8.GetBytes(value, buffer.ToSpan());
            buffer.Truncate(length);
            _stringValues.Add(buffer);
            _count.Strings++;
        }

        public void Write(string path, int field, string value, long longValue, double doubleValue, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            Write(path, field, value, ref entryWriter);
            _longValues.Add(longValue);
            _doubleValues.Add(doubleValue);
            _count.Longs++;
            _count.Doubles++;
        }

        public void Write(string path, int field, BlittableJsonReaderObject reader, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            _blittableJsonReaderObjects.Add(reader);
            _count.Raws++;
        }

        public void Write(string path, int field, CoraxSpatialPointEntry entry, ref IndexEntryWriter entryWriter)
        {
            if (_isDynamic)
                FlushWhenNecessary(path, field, ref entryWriter);
            
            _count.Spatials++;
            _spatialValues.Add(entry);

        }
        
        public void Finish(string path, int field, ref IndexEntryWriter entryWriter)
        {
            var dataType = GetDataType();

            if (_isDynamic || field == global::Corax.Constants.IndexWriter.DynamicField)
            {
                dataType |= DataType.Dynamic;
                path = _persistedName ?? path;
                
                //Look comment in DataType definition. In this case we can do this because is always going to EnumerableScope from dynamic by design.
                if (_count.Strings == 1 || _count.Raws == 1)
                {
                    dataType |= DataType.Single;
                    if (_hasNulls)
                        dataType |= DataType.Null;
                }
            }
            
            switch (dataType)
            {
                case DataType.SingleSpatial:
                    entryWriter.WriteSpatial(field, _spatialValues[0]);
                    break;
                case DataType.DynamicSingleSpatial:
                    entryWriter.WriteSpatialDynamic(path, _spatialValues[0]);
                    break;

                case DataType.Spatials:
                    entryWriter.WriteSpatial(field, CollectionsMarshal.AsSpan(_spatialValues));
                    break;
                case DataType.DynamicSpatials:
                    entryWriter.WriteSpatialDynamic(path, CollectionsMarshal.AsSpan(_spatialValues));
                    break;


                case DataType.SingleTuple:
                    entryWriter.Write(field, _stringValues[0].ToSpan(), _longValues[0], _doubleValues[0]);
                    break;

                case DataType.DynamicSingleTuple:
                    entryWriter.WriteDynamic(path, _stringValues[0].ToSpan(), _longValues[0], _doubleValues[0]);
                    break;

                case DataType.Tuples:
                    entryWriter.Write(field, new ByteStringIterator(_stringValues), CollectionsMarshal.AsSpan(_longValues), CollectionsMarshal.AsSpan(_doubleValues));
                    break;

                case DataType.DynamicTuples:
                    entryWriter.WriteDynamic(path, new ByteStringIterator(_stringValues), CollectionsMarshal.AsSpan(_longValues), CollectionsMarshal.AsSpan(_doubleValues));
                    break;


                case DataType.SingleString:
                    entryWriter.Write(field, _stringValues[0].ToSpan());
                    break;
                
                case DataType.SingleStringNull:
                    entryWriter.WriteNull(field);
                    break;
                
                case DataType.DynamicSingleStringNull:
                    entryWriter.WriteNullDynamic(path);
                    break;
                
                case DataType.DynamicSingleString:
                    entryWriter.WriteDynamic(path, _stringValues[0].ToSpan());
                    break;

                case DataType.Strings:
                    entryWriter.Write(field, new ByteStringIterator(_stringValues));
                    break;

                case DataType.DynamicStrings:
                    entryWriter.WriteDynamic(path, new ByteStringIterator(_stringValues));
                    break;


                case DataType.DynamicSingleRaw:
                case DataType.SingleRaw:
                    new BlittableWriterScope(_blittableJsonReaderObjects[0]).Write(path, field, ref entryWriter);
                    break;

                case DataType.Raws:
                    entryWriter.Write(field, new BlittableIterator(_blittableJsonReaderObjects), IndexEntryFieldType.Raw);
                    break;

                case DataType.DynamicRaws:
                    entryWriter.WriteDynamic(path, new BlittableIterator(_blittableJsonReaderObjects), IndexEntryFieldType.Raw);
                    break;


                case DataType.Empty:
                    //do nothing;
                    break;
                default:
                    ThrowMixedValues();
                    break;
            }

            DisposeStringsCollection();
            ClearContainers();
            

            void ClearContainers()
            {
                _hasNulls = false;
                _count = (0, 0, 0, 0, 0);
                _stringValues.Clear();
                _longValues.Clear();
                _doubleValues.Clear();
                _blittableJsonReaderObjects.Clear();
                _spatialValues.Clear();
            }
            
            void ThrowMixedValues()
            {
                throw new InvalidDataException($"{nameof(EnumerableWriterScope)}: Some raws were mixed with normal literal.");
            }

            void DisposeStringsCollection()
            {
                var stringSpan = CollectionsMarshal.AsSpan(_stringValues);
                for (int i = 0; i < _stringValues.Count; i++)
                {
                    ref var item = ref stringSpan[i];
                    if (item.HasValue)
                        _allocator.Release(ref item);
                }
            }
        }

        public void SetAsDynamic()
        {
            _isDynamic = true;
        }
        
        private DataType GetDataType()
        {
            var type = DataType.Empty;
            if (_count.Strings > 0)
            {
                type |= DataType.Strings;
            }

            if (_count.Longs > 0)
            {
                type |= DataType.Longs;
            }

            if (_count.Doubles > 0)
            {
                type |= DataType.Doubles;
            }

            if (_count.Spatials > 0)
            {
                type |= _count.Spatials == 1 
                    ? DataType.SingleSpatial 
                    : DataType.Spatials;
            }

            if (_count.Raws > 0)
            {
                type |= _count.Raws == 1 
                    ? DataType.SingleRaw 
                    : DataType.Raws;
            }

            if (type == DataType.Tuples)
            {
                var isTuple = _count.Longs == _count.Strings && _count.Longs == _count.Doubles;
                if (isTuple == false)
                {
                    type = DataType.Strings; //case when at least one item is only string. In such case lets write all data as strings.
                }
            }


            return type;
        }

        [Flags]
        private enum DataType : short
        {
            Empty = 0,
            Strings = 1 << 1,
            Doubles = 1 << 2,
            Longs = 1 << 3,
            Spatials = 1 << 4,
            Raws = 1 << 5,

            Single = 1 << 6,
            Dynamic = 1 << 7,
            Null = 1 << 8,
            
            Tuples = Strings | Doubles | Longs,

            SingleTuple = Tuples | Single,
            SingleString = Strings | Single, // do not use it, our projections code is invalid with it. We don't have _IsArray marker in Corax so we've to save it as a list.
            SingleRaw = Raws | Single,
            SingleSpatial = Spatials | Single,

            DynamicSingleTuple = DynamicTuples | Single,
            DynamicSingleString = DynamicStrings | Single,
            DynamicSingleRaw = DynamicRaws | Single,
            DynamicSingleSpatial = DynamicSpatials | Single,
            
            DynamicStrings = Strings | Dynamic,
            DynamicTuples = Tuples | Dynamic, // do not use it, our projections code is invalid with it. We don't have _IsArray marker in Corax so we've to save it as a list.
            DynamicRaws = Raws | Dynamic,
            DynamicSpatials = Spatials | Dynamic,
            
            SingleStringNull = SingleString | Null,
            DynamicSingleStringNull = DynamicSingleString | Null,
            

            
        }
    }
}
