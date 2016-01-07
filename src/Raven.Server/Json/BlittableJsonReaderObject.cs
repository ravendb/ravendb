using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using ConsoleApplication4;
using Raven.Json.Linq;
using Raven.Server.Json;

namespace NewBlittable
{
    public class BlittableJsonReaderObject : BlittableJsonReaderBase
    {
        private readonly unsafe byte* _propTags;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly unsafe byte* _objStart;
        private Dictionary<int, string> _propertyNames;

        private Dictionary<string, Tuple<object, BlittableJsonToken>> _objectsPathCache;


        public unsafe BlittableJsonReaderObject(byte* mem, int size, RavenOperationContext context)
        {
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            // init document level properties
            var propStartPos = size - sizeof (int) - sizeof (byte); //get start position of properties
            _propNames = (mem + (*(int*) (mem + propStartPos)));
            var propNamesOffsetFlag = (BlittableJsonToken) (*(byte*) _propNames);
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
            var objStartOffset = *(int*) (mem + (size - sizeof (int) - sizeof (int) - sizeof (byte)));
            // get offset of beginning of data of the main object
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(objStartOffset, out propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _propTags = objStartOffset + mem + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonToken) (*(mem + size - sizeof (byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(currentType);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(currentType);
        }

        public unsafe BlittableJsonReaderObject(int pos, BlittableJsonReaderBase parent, BlittableJsonToken type)
        {
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            var propNamesOffsetFlag = (BlittableJsonToken) (*(byte*) _propNames);
            switch (propNamesOffsetFlag)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    _propNamesDataOffsetSize = sizeof (byte);
                    break;
                case BlittableJsonToken.OffsetSizeShort:
                    _propNamesDataOffsetSize = sizeof (short);
                    break;
                case BlittableJsonToken.PropertyIdSizeInt:
                    _propNamesDataOffsetSize = sizeof (int);
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

            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof (byte));

            // Prepare an array of all offsets and property ids
            for (var i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = (long) _propTags + (i)*metadataSize;
                var propertyId = ReadNumber((byte*) propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var propertyOffset = ReadNumber((byte*) propertyIntPtr, _currentOffsetSize);
                idsAndOffsets[i] = new BlittableJsonWriter.PropertyTag
                {
                    Position = propertyOffset,
                    PropertyId = propertyId
                };
            }

            // sort according to offsets
            Array.Sort(idsAndOffsets, (tag1, tag2) => tag2.Position - tag1.Position);

            // generate string array, sorted according to it's offsets
            for (int i = 0; i < _propCount; i++)
            {
                sortedNames[i] = GetPropertyName(idsAndOffsets[i].PropertyId);
            }
            return sortedNames;
        }

        private unsafe string GetPropertyName(int propertyId)
        {
            if(_propertyNames == null)
                _propertyNames = new Dictionary<int, string>();

            string value;
            if (_propertyNames.TryGetValue(propertyId, out value) == false)
            {
                var propertyNameOffsetPtr = _propNames + 1 + propertyId*_propNamesDataOffsetSize;
                var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

                // Get the relative "In Document" position of the property Name
                var propRelativePos = _propNames - propertyNameOffset - _mem;

                _propertyNames[propertyId] = value = ReadStringLazily((int) propRelativePos);
            }

            return value;
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
            Tuple<object, BlittableJsonToken> res;
            var found = TryGetMemberAsTypeValueTuple(name, out res);
            result = res.Item1;
            return found;
        }


        public unsafe bool TryGetMemberAsTypeValueTuple(string name, out Tuple<object, BlittableJsonToken> result)
        {
            result = null;
            int min = 0, max = _propCount;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (_objectsPathCache != null && _objectsPathCache.TryGetValue(name, out result))
                return true;

            var comparer = _context.GetComparerFor(name);

            while (min <= max)
            {
                var mid = (min + max)/2;

                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof (byte));
                var propertyIntPtr = (long) _propTags + (mid)*metadataSize;

                var offset = ReadNumber((byte*) propertyIntPtr, _currentOffsetSize);
                var propertyId = ReadNumber((byte*) propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type =
                    (BlittableJsonToken)
                        ReadNumber((byte*) (propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize),
                            _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, comparer);
                if (cmpResult == 0)
                {
                    // found it...
                    result = Tuple.Create(GetObject(type, (int) ((long) _objStart - (long) _mem - (long) offset)),
                        type & typesMask);
                    if (result.Item1 is BlittableJsonReaderBase)
                    {
                        if (_objectsPathCache == null)
                        {
                            _objectsPathCache = new Dictionary<string, Tuple<object, BlittableJsonToken>>();
                        }
                        _objectsPathCache[name] = result;
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
        /// Compares property names between received StringToByteComparer and the string stored in the document's propery names storage
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
            var properyNameRelativePaosition = _propNames - propertyNameOffset;
            var position = properyNameRelativePaosition - _mem;

            byte propertyNameLengthDataLength;

            // Get the propertu name size
            var size = ReadVariableSizeInt((int) position, out propertyNameLengthDataLength);

            // Return result of comparison between proprty name and received comparer
            return comparer.Compare(properyNameRelativePaosition + propertyNameLengthDataLength, size);
        }


        public void WriteTo(TextWriter writer)
        {
            var propertyNames = GetPropertyNames();
            writer.Write('{');
            for (int index   = 0; index < propertyNames.Length; index++)
            {
                var propertyName = propertyNames[index];

                writer.Write('"');
                writer.Write(propertyName);
                writer.Write("\":");

                // get field value
                Tuple<object, BlittableJsonToken> propertyValueAndType;
                if (TryGetMemberAsTypeValueTuple(propertyName, out propertyValueAndType) == false)
                    throw new DataMisalignedException($"Blttable Document could not find field {propertyName}");

                // write field value
                WriteValue(writer, propertyValueAndType.Item2, propertyValueAndType.Item1);

                if (index < propertyNames.Length - 1)
                {
                    writer.Write(',');
                }
            }
            writer.Write('}');
        }

        private void WriteValue(TextWriter writer, BlittableJsonToken token, object val)
        {
            switch (token)
            {
                case BlittableJsonToken.StartArray:
                    WriteArrayToStream((BlittableJsonReaderArray) val, writer);
                    break;
                case BlittableJsonToken.StartObject:
                    ((BlittableJsonReaderObject) val).WriteTo(writer);
                    break;
                case BlittableJsonToken.String:
                    writer.Write((string) (LazyStringValue)val);
                    break;
                case BlittableJsonToken.CompressedString:
                    writer.Write((string)(LazyCompressedStringValue)val);
                    break;
                case BlittableJsonToken.Integer:
                    writer.Write((long) val);
                    break;
                case BlittableJsonToken.Float:
                    writer.Write((double) token);
                    break;
                case BlittableJsonToken.Boolean:
                    writer.Write((bool) val ? "true" : "false");
                    break;
                case BlittableJsonToken.Null:
                    writer.Write("null");
                    break;
                default:
                    throw new DataMisalignedException($"Unidentified Type {token}");
            }
        }

        private void WriteArrayToStream(BlittableJsonReaderArray blittableArray, TextWriter writer)
        {
            writer.Write('[');
            var length = blittableArray.Length;
            for (var i = 0; i < length; i++)
            {
                Tuple<object, BlittableJsonToken> propertyValueAndType;
                if (blittableArray.TryGetValueTokenTupleByIndex(i, out propertyValueAndType) == false)
                    throw new DataMisalignedException($"Index {i} not found in array");

               // write field value
                WriteValue(writer, propertyValueAndType.Item2, propertyValueAndType.Item1);

                if (i < length - 1)
                {
                    writer.Write(',');
                }
            }
            writer.Write(']');
        }
    }
}