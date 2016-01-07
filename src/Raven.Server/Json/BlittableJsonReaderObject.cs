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


        private Dictionary<string, Tuple<object, BlittableJsonToken>> cache;


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
                // Get the offset of the property name from the _proprNames position
                var propertyNameOffsetPtr = _propNames + 1 + idsAndOffsets[i].PropertyId * _propNamesDataOffsetSize;
                var propertyNameOffset = ReadNumber(propertyNameOffsetPtr, _propNamesDataOffsetSize);

                // Get the relative "In Document" position of the property Name
                var properyNameRelativePaosition = _propNames - propertyNameOffset - _mem;
                
                sortedNames[i] = (string) ReadStringLazily((int)properyNameRelativePaosition);
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
            if (cache != null && cache.TryGetValue(name, out result))
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
                        if (cache == null)
                        {
                            cache = new Dictionary<string, Tuple<object, BlittableJsonToken>>();
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

        public async Task WriteAsync(Stream stream)
        {
            // TODO: implement better!

            var bytes = Encoding.UTF8.GetBytes("Some JSON goes here");
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }

        public static class JSONConstantsAsBytes
        {
            public static byte[] ObjectStart;
            public static byte[] ObjectEnd;

            public static byte[] ArrayStart;
            public static byte[] ArrayEnd;

            public static byte[] StringStart;
            public static byte[] ValueStart;
            public static byte[] Comma;

            static JSONConstantsAsBytes()
            {
                var encoding = UTF32Encoding.UTF32;
                ObjectStart = encoding.GetBytes("{");
                ObjectEnd = encoding.GetBytes("}");
                ArrayStart = encoding.GetBytes("[");
                ArrayEnd = encoding.GetBytes("]");
                StringStart = encoding.GetBytes("");
                ValueStart = encoding.GetBytes(":");
                Comma = encoding.GetBytes(",");
            }
        }

        public async Task WriteObjectAsJsonStringAsync(Stream stream)
        {
            // TODO: implement better!
            var bufferSize = 8;
            byte[] numericValueBytes;
            var propertyNames = GetPropertyNames();
            stream.WriteAsync(JSONConstantsAsBytes.ObjectStart, 0, JSONConstantsAsBytes.ObjectStart.Length);
            for (int index   = 0; index < propertyNames.Length; index++)
            {
                var propertyName = propertyNames[index];
                
                // write field start
                await stream.WriteAsync(JSONConstantsAsBytes.StringStart, 0, JSONConstantsAsBytes.StringStart.Length);
                var nameUnicodeByteArray = _context.GetUnicodeByteArrayForFieldName(propertyName);
                await stream.WriteAsync(nameUnicodeByteArray, 0, nameUnicodeByteArray.Length);
                await stream.WriteAsync(JSONConstantsAsBytes.StringStart, 0, JSONConstantsAsBytes.StringStart.Length);
                await stream.WriteAsync(JSONConstantsAsBytes.ValueStart, 0, JSONConstantsAsBytes.ValueStart.Length);

                // get field value
                Tuple<object, BlittableJsonToken> propertyValueAndType;
                if (TryGetMemberAsTypeValueTuple(propertyName, out propertyValueAndType) == false)
                    throw new DataMisalignedException($"Blttable Document could not find field {propertyName}");

                // wrire field value
                switch (propertyValueAndType.Item2)
                {
                    case BlittableJsonToken.StartArray:
                        await WriteArrayToStreamAsync((BlittableJsonReaderArray) propertyValueAndType.Item1, stream);
                        break;
                    case BlittableJsonToken.StartObject:
                        var obj = (BlittableJsonReaderObject) propertyValueAndType.Item1;
                        await obj.WriteObjectAsJsonStringAsync(stream);
                        break;
                    case BlittableJsonToken.String:
                    case BlittableJsonToken.CompressedString:
                        await WriteUnicodeStringToStreamAsync((string) propertyValueAndType.Item1, stream);
                        break;
                    // todo: write numbers more efficiently
                    case BlittableJsonToken.Integer:
                        numericValueBytes = BitConverter.GetBytes((int) propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Float:
                        numericValueBytes = BitConverter.GetBytes((float) propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Boolean:
                        numericValueBytes = BitConverter.GetBytes((byte) propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Null:
                        // not sure about that
                        numericValueBytes = new byte[1] {0};
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    default:
                        throw new DataMisalignedException($"Unidentified Type P{propertyValueAndType.Item2}");
                }

                if (index < propertyNames.Length - 1)
                {
                    stream.WriteAsync(JSONConstantsAsBytes.Comma, 0, JSONConstantsAsBytes.Comma.Length);
                }
            }
            await stream.WriteAsync(JSONConstantsAsBytes.ObjectEnd, 0, JSONConstantsAsBytes.ObjectEnd.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async Task WriteUnicodeStringToStreamAsync(string value, Stream stream)
        {
            // todo: consider implementing receiving LazyString value 
            var buffer = _context.EncodingUnicode.GetBytes(value);
            await stream.WriteAsync(buffer, 0, buffer.Length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async Task WriteArrayToStreamAsync(BlittableJsonReaderArray blittableArray, Stream stream)
        {
            byte[] numericValueBytes;
            await stream.WriteAsync(JSONConstantsAsBytes.ArrayStart, 0, JSONConstantsAsBytes.ArrayStart.Length);
            // todo: consider implementing receiving LazyString value 
            for (var i = 0; i < blittableArray.Length; i++)
            {
                Tuple<object, BlittableJsonToken> propertyValueAndType;
                if (blittableArray.TryGetValueTokenTupleByIndex(i, out propertyValueAndType) == false)
                    throw new DataMisalignedException($"Index {i} not found in array");

                switch (propertyValueAndType.Item2)
                {
                    case BlittableJsonToken.StartArray:
                        throw new DataMisalignedException($"Cannot have array inside array in a JSON");
                        break;
                    case BlittableJsonToken.StartObject:
                        var obj = (BlittableJsonReaderObject)propertyValueAndType.Item1;
                        await obj.WriteObjectAsJsonStringAsync(stream);
                        break;
                    case BlittableJsonToken.String:
                    case BlittableJsonToken.CompressedString:
                        await WriteUnicodeStringToStreamAsync((string)propertyValueAndType.Item1, stream);
                        break;
                    // todo: write numbers more efficiently
                    case BlittableJsonToken.Integer:
                        numericValueBytes = BitConverter.GetBytes((int)propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Float:
                        numericValueBytes = BitConverter.GetBytes((float)propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Boolean:
                        numericValueBytes = BitConverter.GetBytes((byte)propertyValueAndType.Item2);
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    case BlittableJsonToken.Null:
                        // not sure about that
                        numericValueBytes = new byte[1] { 0 };
                        await stream.WriteAsync(numericValueBytes, 0, numericValueBytes.Length);
                        break;
                    default:
                        throw new DataMisalignedException($"Unidentified Type P{propertyValueAndType.Item2}");
                }

                if (i < blittableArray.Length - 1)
                {
                    await stream.WriteAsync(JSONConstantsAsBytes.Comma, 0, JSONConstantsAsBytes.Comma.Length);
                }
            }

            await stream.WriteAsync(JSONConstantsAsBytes.ArrayEnd, 0, JSONConstantsAsBytes.ArrayEnd.Length);
        }
    }
}