using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;

namespace Raven.Server.Json
{
    public class BlittableJsonReaderObject : BlittableJsonReaderBase
    {
        private unsafe readonly byte* _propTags;
        private unsafe readonly byte* _objStart;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        

        private Dictionary<string, object> cache;

        public unsafe BlittableJsonReaderObject(byte* mem, int size, RavenOperationContext context)
        {
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            // init document level properties
            var propStartPos = size - sizeof(int) - sizeof(byte); //get start position of properties
            _propNames = (mem + (*(int*)(mem + propStartPos)));
            var propNamesOffsetFlag = (BlittableJsonToken )(* (byte*) _propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof (byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof (short);
                    break;
                case BlittableJsonToken.OffsetSizeInt:
                    _propNamesDataOffsetSize = sizeof (int);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Property names offset flag should be either byte, short of int, instead of {propNamesOffsetFlag}");
            }
            // get pointer to property names array on document level

            // init root level object properties
            var objStartOffset = *(int*)(mem + (size - sizeof(int) - sizeof(int) - sizeof(byte)));
            // get offset of beginning of data of the main object
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(objStartOffset, out propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _propTags = objStartOffset + mem + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonToken)(*(mem + size - sizeof(byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(currentType);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(currentType);
        }

        internal unsafe BlittableJsonReaderObject(int pos, BlittableJsonReaderBase parent, BlittableJsonToken type)
        {
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            var propNamesOffsetFlag = (BlittableJsonToken)(*(byte*)_propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof(byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof(short);
                    break;
                case BlittableJsonToken.PropertyIdSizeInt:
                    _propNamesDataOffsetSize = sizeof(int);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(
                        $"Property names offset flag should be either byte, short of int, instead of {propNamesOffsetFlag}");
            }

            _objStart = _mem + pos;
            byte propCountOffset;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _propTags = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

        
        /// <summary>
        /// Returns an array of property names, ordered in the order it was stored 
        /// </summary>
        /// <returns></returns>
        public unsafe string[] GetPropertyNames()
        {
            var idsAndOffsets = new BlittableJsonWriter.PropertyTag[_propCount];
            var sortedNames = new string[_propCount];
            
            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
            
            // Prepare an array of all offsets and property ids
            for (var i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = (long)_propTags + (i) * metadataSize;
                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var propertyOffset = ReadNumber((byte*)propertyIntPtr, _currentOffsetSize);
                idsAndOffsets[i] = new BlittableJsonWriter.PropertyTag
                {
                    Position = propertyOffset,
                    PropertyId = propertyId
                };
            }

            // sort according to offsets
            Array.Sort(idsAndOffsets,(tag1, tag2)=> tag2.Position- tag1.Position);

            // generate string array, sorted according to it's offsets
            for (int i = 0; i < _propCount; i++)
            {
                sortedNames[i] = (string) ReadStringLazily(_propNames[idsAndOffsets[i].PropertyId]);
            }
            return sortedNames;
        }

        public RavenJObject GenerateRavenJObject()
        {
            // TODO: Implement!
            return null;
        }

        public object this[string name]
        {
            get
            {
                object result = null;
                if (TryGetMember(name, out result) == false)
                    throw new ArgumentException($"Member named {name} does not exist");
                return result;
            }
        }

        public unsafe bool TryGetMember(string name, out object result)
        {
            result = null;
            int min = 0, max = _propCount;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (cache != null && cache.TryGetValue(name, out result))
                return true;

            var comparer = _context.GetComparerFor(name);

            while (min <= max)
            {
                var mid = (min + max) / 2;

                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
                var propertyIntPtr = (long)_propTags + (mid) * metadataSize;

                var offset = ReadNumber((byte*)propertyIntPtr, _currentOffsetSize);
                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type =
                    (BlittableJsonToken)
                        ReadNumber((byte*)(propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize),
                            _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, comparer);
                if (cmpResult == 0)
                {
                    // found it...
                    result = GetObject(type, (int)((long)_objStart - (long)_mem - (long)offset));
                    if (result is BlittableJsonReaderBase)
                    {
                        if (cache == null)
                        {
                            cache = new Dictionary<string, object>();
                        }
                        cache[name] = result;
                    }
                    return true;
                }
                if (cmpResult > 0)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
            }
           
            return false;
        }

        /// <summary>
        /// Compares property names between received LazyStringValue and the string stored in the document's propery names storage
        /// </summary>
        /// <param name="propertyId">Position of the string in the property ids storage</param>
        /// <param name="comparer">Comparer of a specific string value</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe int ComparePropertyName(int propertyId, LazyStringValue comparer)
        {
            // Get the offset of the property name from the _proprNames position
            var propertyNameOffsetPtr = _propNames + 1 + propertyId*_propNamesDataOffsetSize;
            var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

            // Get the relative "In Document" position of the property Name
            var properyNameRelativePaosition = _propNames  - propertyNameOffset;
            var position = properyNameRelativePaosition - _mem;

            byte propertyNameLengthDataLength;

            // Get the propertu name size
            var size = ReadVariableSizeInt((int)position, out propertyNameLengthDataLength);

            // Return result of comparison between proprty name and received comparer
            return comparer.Compare(properyNameRelativePaosition + propertyNameLengthDataLength, size);
        }

        public async Task WriteAsync(Stream stream)
        {
            // TODO: implement better!

            var bytes = Encoding.UTF8.GetBytes("Some JSON goes here");
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }
    }
}