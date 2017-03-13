using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Sparrow.Collections;
using Sparrow.Global;
using Sparrow.Json.Parsing;

namespace Sparrow.Json
{
    public class BlittableJsonDocumentBuilder : IDisposable
    {
        private static readonly StringSegment UnderscoreSegment = new StringSegment("_");

        protected readonly FastStack<BuildingState> _continuationState = new FastStack<BuildingState>();

        protected readonly JsonOperationContext _context;
        private UsageMode _mode;
        private readonly IJsonParser _reader;
        private readonly IBlittableDocumentModifier _modifier;
        private readonly BlittableWriter<UnmanagedWriteBuffer> _writer;
        private readonly JsonParserState _state;

        protected WriteToken _writeToken;
        private  string _debugTag;

        private readonly ListCache<PropertyTag> _propertiesCache = new ListCache<PropertyTag>();
        private readonly ListCache<int> _positionsCache = new ListCache<int>();
        private readonly ListCache<BlittableJsonToken> _tokensCache = new ListCache<BlittableJsonToken>();

        private class ListCache<T>
        {
            private readonly FastList<FastList<T>> _cache = new FastList<FastList<T>>();
            private int _index = 0;

            public FastList<T> Allocate()
            {
                if (_index == _cache.Count)
                {
                    var n = new FastList<T>();
                    _cache.Add(n);
                    _index++;
                    return n;
                }
                return _cache[_index++];
            }

            public void Reset()
            {
                for (var i = 0; i < _index; i++)
                {
                    var t = _cache[i];
                    t.Clear();
                }
                _index = 0;
            }
        }


        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, IJsonParser reader,
            BlittableWriter<UnmanagedWriteBuffer> writer = null,
            IBlittableDocumentModifier modifier = null)
        {
            _context = context;
            _state = state;
            _reader = reader;
            _modifier = modifier;
            _writer = writer ?? new BlittableWriter<UnmanagedWriteBuffer>(context);
        }

        public BlittableJsonDocumentBuilder(
            JsonOperationContext context, 
            UsageMode mode, string debugTag, 
            IJsonParser reader, JsonParserState state, 
            BlittableWriter<UnmanagedWriteBuffer> writer = null,
            IBlittableDocumentModifier modifier = null) : this(context, state, reader, writer, modifier)
        {
            Renew(debugTag, mode);
        }

        public BlittableJsonDocumentBuilder(JsonOperationContext context, JsonParserState state, UsageMode mode, string debugTag, IJsonParser reader, BlittableWriter<UnmanagedWriteBuffer> writer = null) : this(context, state, reader, writer)
        {
            Renew(debugTag, mode);
        }

        public void Reset()
        {
            _debugTag = null;
            _mode = UsageMode.None;
            _continuationState.Clear();
            _writeToken = default(WriteToken);
            _writer.Reset();
            ResetCaches();
        }

        public void Renew(string debugTag, UsageMode mode)
        {
            Reset();
            _debugTag = debugTag;
            _mode = mode;
            _writer.ResetAndRenew();
        }

        public virtual void ReadArrayDocument()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadArrayDocument
            });
        }

        public virtual void ReadObjectDocument()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadObjectDocument
            });
        }

        public virtual void ReadNestedObject()
        {
            _continuationState.Push(new BuildingState
            {
                State = ContinuationState.ReadObject
            });
        }

        public int SizeInBytes => _writer.SizeInBytes;


        public void Dispose()
        {
            _writer.Dispose();
        }

        public virtual bool Read()
        {
            if (_continuationState.Count == 0)
                return false; //nothing to do

            var currentState = _continuationState.Pop();
            while (true)
            {
                switch (currentState.State)
                {
                    case ContinuationState.ReadObjectDocument:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.ReadObject;
                        continue;
                    case ContinuationState.ReadArrayDocument:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }

                        var fakeFieldName = _context.GetLazyStringForFieldWithCaching(UnderscoreSegment);
                        var prop = _context.CachedProperties.GetProperty(fakeFieldName);
                        currentState.CurrentProperty = prop;
                        currentState.MaxPropertyId = prop.PropertyId;
                        currentState.FirstWrite = _writer.Position;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.Properties.Add(
                            new PropertyTag
                            {
                                Property = prop
                            }
                        );
                        currentState.State = ContinuationState.CompleteDocumentArray;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadArray
                        };
                        continue;
                    case ContinuationState.CompleteDocumentArray:
                        currentState.Properties[0] = new PropertyTag(                        
                            type: (byte)_writeToken.WrittenToken,
                            property: currentState.Properties[0].Property,                           
                            position: _writeToken.ValuePos
                        );

                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);

                        return true;
                    case ContinuationState.ReadObject:
                        if (_state.CurrentTokenType != JsonParserToken.StartObject)
                            ThrowExpectedStartOfObject();
                        currentState.State = ContinuationState.ReadPropertyName;
                        currentState.Properties = _propertiesCache.Allocate();
                        currentState.FirstWrite = _writer.Position;
                        continue;
                    case ContinuationState.ReadArray:
                        if (_state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowExpectedStartOfArray();
                        currentState.Types = _tokensCache.Allocate();
                        currentState.Positions = _positionsCache.Allocate();
                        currentState.State = ContinuationState.ReadArrayValue;
                        continue;
                    case ContinuationState.ReadArrayValue:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        if (_state.CurrentTokenType == JsonParserToken.EndArray)
                        {
                            currentState.State = ContinuationState.CompleteArray;
                            continue;
                        }
                        currentState.State = ContinuationState.CompleteArrayValue;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadValue
                        };
                        continue;
                    case ContinuationState.CompleteArrayValue:
                        currentState.Types.Add(_writeToken.WrittenToken);
                        currentState.Positions.Add(_writeToken.ValuePos);
                        currentState.State = ContinuationState.ReadArrayValue;
                        continue;
                    case ContinuationState.CompleteArray:

                        var arrayToken = BlittableJsonToken.StartArray;
                        var arrayInfoStart = _writer.WriteArrayMetadata(currentState.Positions, currentState.Types, ref arrayToken);

                        _writeToken = new WriteToken
                        {
                            ValuePos = arrayInfoStart,
                            WrittenToken = arrayToken
                        };

                        currentState = _continuationState.Pop();
                        continue;
                    case ContinuationState.ReadPropertyName:
                        if (ReadMaybeModifiedPropertyName() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }

                        if (_state.CurrentTokenType == JsonParserToken.EndObject)
                        {
                            _modifier?.EndObject();
                            _writeToken = _writer.WriteObjectMetadata(currentState.Properties, currentState.FirstWrite, currentState.MaxPropertyId);
                            if (_continuationState.Count == 0)
                                return true;
                            currentState = _continuationState.Pop();
                            continue;
                        }

                        if (_state.CurrentTokenType != JsonParserToken.String)
                            ThrowExpectedProperty();

                        var property = CreateLazyStringValueFromParserState();

                        currentState.CurrentProperty = _context.CachedProperties.GetProperty(property);
                        currentState.MaxPropertyId = Math.Max(currentState.MaxPropertyId, currentState.CurrentProperty.PropertyId);
                        currentState.State = ContinuationState.ReadPropertyValue;
                        continue;
                    case ContinuationState.ReadPropertyValue:
                        if (_reader.Read() == false)
                        {
                            _continuationState.Push(currentState);
                            return false;
                        }
                        currentState.State = ContinuationState.CompleteReadingPropertyValue;
                        _continuationState.Push(currentState);
                        currentState = new BuildingState
                        {
                            State = ContinuationState.ReadValue
                        };
                        continue;
                    case ContinuationState.CompleteReadingPropertyValue:
                        // Register property position, name id (PropertyId) and type (object type and metadata)
                        currentState.Properties.Add(new PropertyTag
                        {
                            Position = _writeToken.ValuePos,
                            Type = (byte)_writeToken.WrittenToken,
                            Property = currentState.CurrentProperty
                        });
                        currentState.State = ContinuationState.ReadPropertyName;
                        continue;
                    case ContinuationState.ReadValue:
                        ReadJsonValue();
                        currentState = _continuationState.Pop();
                        break;
                }
            }
        }

        private bool ReadMaybeModifiedPropertyName()
        {
            if (_modifier != null)
            {
                return _modifier.AboutToReadPropertyName(_reader, _state);
            }
            return _reader.Read();
        }

        private void ThrowExpectedProperty()
        {
            throw new InvalidDataException("Expected property, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfArray()
        {
            throw new InvalidDataException("Expected start of array, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private void ThrowExpectedStartOfObject()
        {
            throw new InvalidDataException("Expected start of object, but got " + _state.CurrentTokenType + _reader.GenerateErrorState());
        }

        private unsafe void ReadJsonValue()
        {
            int start;
            switch (_state.CurrentTokenType)
            {
                case JsonParserToken.StartObject:
                    _modifier?.StartObject();
                    _continuationState.Push(new BuildingState
                    {
                        State = ContinuationState.ReadObject
                    });
                    return;
                case JsonParserToken.StartArray:
                    _continuationState.Push(new BuildingState
                    {
                        State = ContinuationState.ReadArray
                    });
                    return;
                case JsonParserToken.Integer:
                    start = _writer.WriteValue(_state.Long);
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Integer
                    };
                    return;
                case JsonParserToken.Float:
                    if ((_mode & UsageMode.ValidateDouble) == UsageMode.ValidateDouble)
                        _reader.ValidateFloat();
                    BlittableJsonToken ignored;

                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, out ignored, _mode, _state.CompressedSize);
                    _state.CompressedSize = null;
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Float
                    };
                    return;
                case JsonParserToken.String:
                    BlittableJsonToken stringToken;
                    start = _writer.WriteValue(_state.StringBuffer, _state.StringSize, _state.EscapePositions, out stringToken, _mode, _state.CompressedSize);
                    _state.CompressedSize = null;
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = stringToken
                    };
                    return;
                case JsonParserToken.True:
                case JsonParserToken.False:
                    start = _writer.WriteValue(_state.CurrentTokenType == JsonParserToken.True ? (byte)1 : (byte)0);
                    _writeToken = new WriteToken
                    {
                        ValuePos = start,
                        WrittenToken = BlittableJsonToken.Boolean
                    };
                    return;
                case JsonParserToken.Null:
                    start = _writer.WriteValue((byte)0);
                    _writeToken = new WriteToken // nothing to do here, we handle that with the token
                    {
                        WrittenToken = BlittableJsonToken.Null,
                        ValuePos = start
                    };
                    return;
                default:
                    throw new InvalidDataException("Expected a value, but got " + _state.CurrentTokenType);
            }
        }


        public enum ContinuationState
        {
            ReadPropertyName,
            ReadPropertyValue,
            ReadArray,
            ReadArrayValue,
            ReadObject,
            ReadValue,
            CompleteReadingPropertyValue,
            ReadObjectDocument,
            ReadArrayDocument,
            CompleteDocumentArray,
            CompleteArray,
            CompleteArrayValue
        }

        public struct BuildingState
        {
            public ContinuationState State;
            public FastList<PropertyTag> Properties;
            public CachedProperties.PropertyName CurrentProperty;
            public int MaxPropertyId;
            public FastList<BlittableJsonToken> Types;
            public FastList<int> Positions;
            public long FirstWrite;
        }


        public struct PropertyTag
        {
            public int Position;

            public override string ToString()
            {
                return $"{nameof(Position)}: {Position}, {nameof(Property)}: {Property.Comparer} {Property.PropertyId}, {nameof(Type)}: {(BlittableJsonToken)Type}";
            }
            public CachedProperties.PropertyName Property;
            public byte Type;

            public PropertyTag(byte type, CachedProperties.PropertyName property, int position)
            {
                this.Type = type;
                this.Property = property;
                this.Position = position;
            }
        }

        [Flags]
        public enum UsageMode
        {
            None = 0,
            ValidateDouble = 1,
            CompressStrings = 2,
            CompressSmallStrings = 4,
            ToDisk = ValidateDouble | CompressStrings
        }

        public struct WriteToken
        {
            public int ValuePos;
            public BlittableJsonToken WrittenToken;
        }


        private unsafe LazyStringValue CreateLazyStringValueFromParserState()
        {
            var lazyStringValueFromParserState = _context.AllocateStringValue(null, _state.StringBuffer, _state.StringSize);

            if (_state.EscapePositions.Count > 0)
            {
                lazyStringValueFromParserState.EscapePositions = _state.EscapePositions.ToArray();
            }
            return lazyStringValueFromParserState;
        }

        protected static int SetOffsetSizeFlag(ref BlittableJsonToken objectToken, long distanceFromFirstProperty)
        {
            int positionSize;
            if (distanceFromFirstProperty <= byte.MaxValue)
            {
                positionSize = sizeof(byte);
                objectToken |= BlittableJsonToken.OffsetSizeByte;
            }
            else
            {
                if (distanceFromFirstProperty <= ushort.MaxValue)
                {
                    positionSize = sizeof(short);
                    objectToken |= BlittableJsonToken.OffsetSizeShort;
                }
                else
                {
                    positionSize = sizeof(int);
                    objectToken |= BlittableJsonToken.OffsetSizeInt;
                }
            }
            return positionSize;
        }

        public virtual void FinalizeDocument()
        {
            var documentToken = _writeToken.WrittenToken;
            var rootOffset = _writeToken.ValuePos;

            _writer.WriteDocumentMetadata(rootOffset, documentToken);
            ResetCaches();
        }

        public BlittableJsonReaderObject CreateReader()
        {
            return _writer.CreateReader();
        }

        private void ResetCaches()
        {
            _propertiesCache.Reset();
            _tokensCache.Reset();
            _positionsCache.Reset();
        }

        public BlittableJsonReaderArray CreateArrayReader(bool noCache)
        {
            var reader = CreateReader();
            reader.NoCache = noCache;
            BlittableJsonReaderArray array;
            if (reader.TryGet("_", out array))
                return array;
            throw new InvalidOperationException("Couldn't find array");
        }

        public override string ToString()
        {
            return "Building json for " + _debugTag;
        }
    }

    public interface IBlittableDocumentModifier
    {
        void StartObject();
        void EndObject();
        bool AboutToReadPropertyName(IJsonParser reader, JsonParserState state);
    }

}