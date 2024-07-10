using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Strings;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents
{
    public sealed class BlittableMetadataModifier : IDisposable, IBlittableDocumentModifier
    {
        private bool _disposed;

        private bool _readingMetadataObject;
        private int _depth;
        private State _state = State.None;
        private bool _verifyStartArray;

        public BlittableMetadataModifier(JsonOperationContext context)
        {
            _ctx = context;
        }

        public BlittableMetadataModifier(JsonOperationContext context, bool legacyImport, bool readLegacyEtag, DatabaseItemType operateOnTypes)
        {
            _ctx = context;
            ReadFirstEtagOfLegacyRevision = legacyImport;
            ReadLegacyEtag = readLegacyEtag;
            OperateOnTypes = operateOnTypes;
        }

        public LazyStringValue Id;
        public string ChangeVector;
        public DocumentFlags Flags;
        public NonPersistentDocumentFlags NonPersistentFlags;
        public DateTime? LastModified;

        // Change vector is null when importing from v3.5.
        // We'll generate a new change vector in this format: "RV:{revisionsCount}-{firstEtagOfLegacyRevision}"
        public bool ReadFirstEtagOfLegacyRevision;

        public bool ReadLegacyEtag;
        public DatabaseItemType OperateOnTypes;
        public string LegacyEtag { get; private set; }
        public string FirstEtagOfLegacyRevision;
        public long LegacyRevisionsCount;

        public bool SeenCounters;
        public bool SeenAttachments;
        public bool SeenTimeSeries;

        private JsonOperationContext _ctx;
        private LazyStringValue _metadataCollections;
        private LazyStringValue _metadataExpires;

        private readonly FastList<AllocatedMemoryData> _allocations = new FastList<AllocatedMemoryData>();

        private const string LegacyLastModified = "Raven-Last-Modified";
        private const string LegacyRevisionState = "Historical";
        private const string LegacyHasRevisionsDocumentState = "Current";

        private DocumentFlags ReadFlags(JsonParserState state)
        {
            var split = CreateLazyStringValueFromParserState(state).Split(',');
            var flags = DocumentFlags.None;
            for (var i = 0; i < split.Length; i++)
            {
                if (Enum.TryParse(split[i], true, out DocumentFlags flag) == false)
                    continue;
                flags |= flag;
            }
            return flags;
        }

        private unsafe DateTime ReadDateTime(JsonParserState jsonParserState, IJsonParser reader, State state)
        {
            var str = CreateLazyStringValueFromParserState(jsonParserState);

            var result = LazyStringParser.TryParseDateTime(str.Buffer, str.Size, out DateTime dt, out DateTimeOffset _, properlyParseThreeDigitsMilliseconds: true);
            if (result != LazyStringParser.Result.DateTime)
                ThrowInvalidLastModifiedProperty(state, str, reader);

            return dt;
        }

        private unsafe LazyStringValue CreateLazyStringValueFromParserState(JsonParserState state)
        {
            int escapePositionsCount = state.EscapePositions.Count;

            var maxSizeOfEscapePos = escapePositionsCount * 5 // max size of var int
                                     + JsonParserState.VariableSizeIntSize(escapePositionsCount);

            var mem = _ctx.GetMemory(maxSizeOfEscapePos + state.StringSize);
            _allocations.Add(mem);
            Memory.Copy(mem.Address, state.StringBuffer, state.StringSize);
            var lazyStringValueFromParserState = _ctx.AllocateStringValue(null, mem.Address, state.StringSize);
            if (escapePositionsCount > 0)
            {
                lazyStringValueFromParserState.EscapePositions = state.EscapePositions.ToArray();
            }
            else
            {
                lazyStringValueFromParserState.EscapePositions = Array.Empty<int>();
            }

            return lazyStringValueFromParserState;
        }

        private enum State
        {
            None,
            ReadingId,
            ReadingFlags,
            ReadingLastModified,
            ReadingLegacyLastModified,
            ReadingLegacyDeleteMarker,
            ReadingChangeVector,
            ReadingFirstEtagOfLegacyRevision,
            ReadingEtag,
            IgnoreProperty,
            IgnoreArray,
            IgnoreRevisionStatusProperty
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StartObject()
        {
            AssertNotDisposed();

            if (_readingMetadataObject == false)
                return;

            _depth++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EndObject()
        {
            AssertNotDisposed();

            if (_readingMetadataObject == false)
                return;

            _depth--;

            Debug.Assert(_depth >= 0);
            if (_depth == 0)
                _readingMetadataObject = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool AboutToReadPropertyName(IJsonParser reader, JsonParserState state)
        {
            AssertNotDisposed();

            if (reader is UnmanagedJsonParser unmanagedParser)
                return AboutToReadPropertyNameInternal(unmanagedParser, state);
            if (reader is ObjectJsonParser objectParser)
                return AboutToReadPropertyNameInternal(objectParser, state);

            return AboutToReadPropertyNameInternal(reader, state);
        }

        private unsafe bool AboutToReadPropertyNameInternal(UnmanagedJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if ("@metadata"u8.IsEqualConstant(state.StringBuffer, state.StringSize) == true)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        private unsafe bool AboutToReadPropertyNameInternal(ObjectJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if ("@metadata"u8.IsEqualConstant(state.StringBuffer, state.StringSize) == true)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        private unsafe bool AboutToReadPropertyNameInternal(IJsonParser reader, JsonParserState state)
        {
            if (_state != State.None)
            {
                if (!AboutToReadWithStateUnlikely(reader, state))
                    return false;
            }

            _state = State.None;

            while (true)
            {
                if (reader.Read() == false)
                    return false;

                if (state.CurrentTokenType != JsonParserToken.String)
                    return true; // let the caller handle that

                if (_readingMetadataObject == false)
                {
                    if ("@metadata"u8.IsEqualConstant(state.StringBuffer, state.StringSize) == true)
                        _readingMetadataObject = true;

                    return true;
                }

                if (AboutToReadPropertyNameInMetadataUnlikely(reader, state, out bool aboutToReadPropertyName))
                    return aboutToReadPropertyName;
            }
        }

        private unsafe bool AboutToReadPropertyNameInMetadataUnlikely(IJsonParser reader, JsonParserState state, out bool aboutToReadPropertyName)
        {
            aboutToReadPropertyName = true;

            switch (state.StringSize)
            {
                default: // accept this property
                    {
                        return true;
                    }
                case -2: // IgnoreArrayProperty
                    {
                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowInvalidArrayType(state, reader);

                        while (state.CurrentTokenType != JsonParserToken.EndArray)
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.IgnoreArray;
                                aboutToReadPropertyName = false;
                                return true;
                            }
                        }
                        break;
                    }
                case -1: // IgnoreProperty
                    {
                        if (reader.Read() == false)
                        {
                            _state = State.IgnoreProperty;
                            aboutToReadPropertyName = false;
                            return true;
                        }
                        if (state.CurrentTokenType == JsonParserToken.StartArray ||
                            state.CurrentTokenType == JsonParserToken.StartObject)
                            ThrowInvalidMetadataProperty(state, reader);
                        break;
                    }

                case 3: // @id

                    if ("@id"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if (reader.Read() == false)
                    {
                        _state = State.ReadingId;
                        aboutToReadPropertyName = false;
                        return true;
                    }
                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Id, state, reader);
                    Id = CreateLazyStringValueFromParserState(state);
                    break;

                case 5: // @etag
                    if ("@etag"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if (ReadFirstEtagOfLegacyRevision &&
                        (NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) == NonPersistentDocumentFlags.LegacyRevision)
                    {
                        if (FirstEtagOfLegacyRevision == null)
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.ReadingFirstEtagOfLegacyRevision;
                                aboutToReadPropertyName = false;
                                return true;
                            }
                            if (state.CurrentTokenType != JsonParserToken.String)
                                ThrowExpectedFieldTypeOfString("@etag", state, reader);
                            FirstEtagOfLegacyRevision = LegacyEtag = CreateLazyStringValueFromParserState(state);
                            ChangeVector = ChangeVectorUtils.NewChangeVector("RV", ++LegacyRevisionsCount, new Guid(FirstEtagOfLegacyRevision).ToBase64Unpadded());
                            break;
                        }

                        ChangeVector = ChangeVectorUtils.NewChangeVector("RV", ++LegacyRevisionsCount, new Guid(FirstEtagOfLegacyRevision).ToBase64Unpadded());
                    }

                    if (ReadLegacyEtag)
                    {
                        if (reader.Read() == false)
                        {
                            _state = State.ReadingEtag;
                            aboutToReadPropertyName = false;
                            return true;
                        }

                        if (state.CurrentTokenType != JsonParserToken.String)
                            ThrowExpectedFieldTypeOfString("@etag", state, reader);
                        LegacyEtag = CreateLazyStringValueFromParserState(state);
                        break;
                    }

                    goto case -1;
                case 6: // @flags
                    if ("@flags"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if (reader.Read() == false)
                    {
                        _state = State.ReadingFlags;
                        aboutToReadPropertyName = false;
                        return true;
                    }
                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Flags, state, reader);
                    Flags = ReadFlags(state);
                    break;

                case 9: // @counters
                    // always remove the @counters metadata
                    // not doing so might cause us to have counter on the document but not in the storage.
                    // the counters will be updated when we import the counters themselves
                    if ("@counters"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    SeenCounters = true;

                    if (reader.Read() == false)
                    {
                        _verifyStartArray = true;
                        _state = State.IgnoreArray;
                        aboutToReadPropertyName = false;
                        return true;
                    }
                    goto case -2;
                case 11: // @timeseries
                    // always remove the @timeseries metadata
                    if ("@timeseries"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    // remove timeseries names from metadata

                    SeenTimeSeries = true;

                    if (reader.Read() == false)
                    {
                        _verifyStartArray = true;
                        _state = State.IgnoreArray;
                        aboutToReadPropertyName = false;
                        return true;
                    }
                    goto case -2;

                case 12: // @index-score OR @attachments
                    if (state.StringBuffer[0] == (byte)'@')
                    {   // @index-score
                        if ("@index-score"u8.IsEqualConstant(state.StringBuffer))
                            goto case -1;

                        // @attachments
                        if ("@attachments"u8.IsEqualConstant(state.StringBuffer))
                        {
                            SeenAttachments = true;
                            if (OperateOnTypes.HasFlag(DatabaseItemType.Attachments) == false)
                            {
                                if (reader.Read() == false)
                                {
                                    _verifyStartArray = true;
                                    _state = State.IgnoreArray;
                                    aboutToReadPropertyName = false;
                                    return true;
                                }
                                goto case -2;
                            }
                        }
                    }

                    aboutToReadPropertyName = true;
                    return true;

                case 13: //Last-Modified
                    if ("Last-Modified"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 14:
                    if (state.StringBuffer[0] == (byte)'@')
                    {
                        // @change-vector
                        if ("@change-vector"u8.IsEqualConstant(state.StringBuffer))
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.ReadingChangeVector;
                                aboutToReadPropertyName = false;
                                return true;
                            }

                            if (state.CurrentTokenType == JsonParserToken.Null)
                                break;

                            if (state.CurrentTokenType != JsonParserToken.String)
                                ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.ChangeVector, state, reader);
                            ChangeVector = CreateLazyStringValueFromParserState(state);
                            break;
                        }

                        // @last-modified
                        if ("@last-modified"u8.IsEqualConstant(state.StringBuffer))
                        {
                            if (reader.Read() == false)
                            {
                                _state = State.ReadingLastModified;
                                aboutToReadPropertyName = false;
                                return true;
                            }
                            if (state.CurrentTokenType != JsonParserToken.String)
                                ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.LastModified, state, reader);
                            LastModified = ReadDateTime(state, reader, State.ReadingLastModified);
                            break;
                        }
                    }

                    aboutToReadPropertyName = true;
                    return true;

                case 15: //Raven-Read-Only
                    if ("Raven-Read-Only"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 17: //Raven-Entity-Name --> @collection
                    if ("Raven-Entity-Name"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var collection = _metadataCollections;
                    state.StringBuffer = collection.AllocatedMemoryData.Address;
                    state.StringSize = collection.Size;
                    aboutToReadPropertyName = true;
                    return true;

                case 19: //Raven-Last-Modified or Raven-Delete-Marker

                    if ("Raven-"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    // PERF: We are aiming to ensure that on Vector128 architectures and AVX2 architecture we use
                    // the vectorial implementation. 
                    if ("en-Last-Modified"u8.IsEqualConstant(state.StringBuffer + "Rav"u8.Length) == false &&
                        "en-Delete-Marker"u8.IsEqualConstant(state.StringBuffer + "Rav"u8.Length) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var isLegacyLastModified = state.StringBuffer[18] == (byte)'d';
                    if (reader.Read() == false)
                    {
                        _state = isLegacyLastModified ? State.ReadingLegacyLastModified : State.ReadingLegacyDeleteMarker;
                        aboutToReadPropertyName = false;
                        return true;
                    }

                    if (isLegacyLastModified)
                    {
                        if (state.CurrentTokenType != JsonParserToken.String)
                            ThrowExpectedFieldTypeOfString(LegacyLastModified, state, reader);

                        LastModified = ReadDateTime(state, reader, State.ReadingLegacyLastModified);
                    }
                    else
                    {
                        if (state.CurrentTokenType == JsonParserToken.True)
                            NonPersistentFlags |= NonPersistentDocumentFlags.LegacyDeleteMarker;
                    }

                    break;

                case 21: //Raven-Expiration-Date
                    if ("Raven-Expiration-Date"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var expires = _metadataExpires;
                    state.StringBuffer = expires.AllocatedMemoryData.Address;
                    state.StringSize = expires.Size;
                    aboutToReadPropertyName = true;
                    return true;

                case 23: //Raven-Document-Revision
                    if ("Raven-Document-Revision"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 24: //Raven-Replication-Source
                    if ("Raven-Replication-Source"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
                case 25: //Raven-Replication-Version OR Raven-Replication-History

                    var lastByte = state.StringBuffer[24];
                    if (lastByte != (byte)'n' && lastByte != (byte)'y')
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    if ("Raven-Replication-Version"u8.IsEqualConstant(state.StringBuffer) == false ||
                        "Raven-Replication-History"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var isReplicationHistory = lastByte == (byte)'y';
                    if (reader.Read() == false)
                    {
                        _verifyStartArray = isReplicationHistory;
                        _state = isReplicationHistory ? State.IgnoreArray : State.IgnoreProperty;
                        aboutToReadPropertyName = false;
                        return true;
                    }

                    // Raven-Replication-History is an array
                    if (isReplicationHistory)
                        goto case -2;

                    if (state.CurrentTokenType == JsonParserToken.StartArray || state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state, reader);
                    break;

                case 29: //Non-Authoritative-Information

                    if ("Non-Authoritative-Information"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;

                case 30: //Raven-Document-Parent-Revision OR Raven-Document-Revision-Status

                    if ("Raven-Document-Parent-Revision"u8.IsEqualConstant(state.StringBuffer) == false &&
                        "Raven-Document-Revision-Status"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    var isRevisionStatusProperty = state.StringBuffer[29] == 's';
                    if (reader.Read() == false)
                    {
                        _state = isRevisionStatusProperty ? State.IgnoreRevisionStatusProperty : State.IgnoreProperty;
                        aboutToReadPropertyName = false;
                        return true;
                    }

                    if (state.CurrentTokenType == JsonParserToken.StartArray ||
                        state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state, reader);

                    if (isRevisionStatusProperty)
                    {
                        switch (CreateLazyStringValueFromParserState(state))
                        {
                            case LegacyHasRevisionsDocumentState:
                                NonPersistentFlags |= NonPersistentDocumentFlags.LegacyHasRevisions;
                                break;

                            case LegacyRevisionState:
                                NonPersistentFlags |= NonPersistentDocumentFlags.LegacyRevision;
                                break;
                        }
                    }

                    break;

                case 32: //Raven-Replication-Merged-History
                    if ("Raven-Replication-Merged-History"u8.IsEqualConstant(state.StringBuffer) == false)
                    {
                        aboutToReadPropertyName = true;
                        return true;
                    }

                    goto case -1;
            }
            return false;
        }

        private bool AboutToReadWithStateUnlikely(IJsonParser reader, JsonParserState state)
        {
            switch (_state)
            {
                case State.None:
                    break;

                case State.IgnoreProperty:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType == JsonParserToken.StartArray ||
                        state.CurrentTokenType == JsonParserToken.StartObject)
                        ThrowInvalidMetadataProperty(state, reader);
                    break;

                case State.IgnoreArray:
                    if (_verifyStartArray)
                    {
                        if (reader.Read() == false)
                            return false;

                        _verifyStartArray = false;

                        if (state.CurrentTokenType != JsonParserToken.StartArray)
                            ThrowInvalidReplicationHistoryType(state, reader);
                    }
                    while (state.CurrentTokenType != JsonParserToken.EndArray)
                    {
                        if (reader.Read() == false)
                            return false;
                    }
                    break;

                case State.IgnoreRevisionStatusProperty:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String &&
                        state.CurrentTokenType != JsonParserToken.Integer)
                        ThrowInvalidEtagType(state, reader);

                    switch (CreateLazyStringValueFromParserState(state))
                    {
                        case LegacyHasRevisionsDocumentState:
                            NonPersistentFlags |= NonPersistentDocumentFlags.LegacyHasRevisions;
                            break;

                        case LegacyRevisionState:
                            NonPersistentFlags |= NonPersistentDocumentFlags.LegacyRevision;
                            break;
                    }
                    break;

                case State.ReadingId:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Id, state, reader);
                    Id = CreateLazyStringValueFromParserState(state);
                    break;

                case State.ReadingFlags:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.Flags, state, reader);
                    Flags = ReadFlags(state);
                    break;

                case State.ReadingLastModified:
                case State.ReadingLegacyLastModified:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.LastModified, state, reader);
                    LastModified = ReadDateTime(state, reader, _state);
                    break;

                case State.ReadingLegacyDeleteMarker:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType == JsonParserToken.True)
                        NonPersistentFlags |= NonPersistentDocumentFlags.LegacyDeleteMarker;

                    break;

                case State.ReadingChangeVector:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType == JsonParserToken.Null)
                        break;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString(Constants.Documents.Metadata.ChangeVector, state, reader);
                    ChangeVector = CreateLazyStringValueFromParserState(state);

                    break;

                case State.ReadingFirstEtagOfLegacyRevision:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString("@etag", state, reader);
                    FirstEtagOfLegacyRevision = LegacyEtag = CreateLazyStringValueFromParserState(state);
                    ChangeVector = ChangeVectorUtils.NewChangeVector("RV", ++LegacyRevisionsCount, new Guid(FirstEtagOfLegacyRevision).ToBase64Unpadded());
                    break;

                case State.ReadingEtag:
                    if (reader.Read() == false)
                        return false;

                    if (state.CurrentTokenType != JsonParserToken.String)
                        ThrowExpectedFieldTypeOfString("@etag", state, reader);
                    LegacyEtag = CreateLazyStringValueFromParserState(state);

                    break;
            }
            return true;
        }

        [DoesNotReturn]
        private void ThrowInvalidMetadataProperty(JsonParserState state, IJsonParser reader)
        {
            throw new InvalidDataException($"Expected property @metadata to be a simple type, but was {state.CurrentTokenType}. Id: '{Id ?? "N/A"}'. Around {reader.GenerateErrorState()}");
        }

        [DoesNotReturn]
        private void ThrowExpectedFieldTypeOfString(string field, JsonParserState state, IJsonParser reader)
        {
            throw new InvalidDataException($"Expected property @metadata.{field} to have string type, but was: {state.CurrentTokenType}. Id: '{Id ?? "N/A"}'. Around: {reader.GenerateErrorState()}");
        }

        [DoesNotReturn]
        private void ThrowInvalidLastModifiedProperty(State state, LazyStringValue str, IJsonParser reader)
        {
            var field = state == State.ReadingLastModified ? Constants.Documents.Metadata.LastModified : LegacyLastModified;
            throw new InvalidDataException($"Cannot parse the value of property @metadata.{field}: {str}. Id: '{Id ?? "N/A"}'. Around: {reader.GenerateErrorState()}");
        }

        [DoesNotReturn]
        private void ThrowInvalidEtagType(JsonParserState state, IJsonParser reader)
        {
            throw new InvalidDataException($"Expected property @metadata.@etag to have string or long type, but was: {state.CurrentTokenType}. Id: '{Id ?? "N/A"}'. Around: {reader.GenerateErrorState()}");
        }

        [DoesNotReturn]
        private void ThrowInvalidReplicationHistoryType(JsonParserState state, IJsonParser reader)
        {
            throw new InvalidDataException($"Expected property @metadata.Raven-Replication-History to have array type, but was: {state.CurrentTokenType}. Id: '{Id ?? "N/A"}'. Around: {reader.GenerateErrorState()}");
        }

        [DoesNotReturn]
        private void ThrowInvalidArrayType(JsonParserState state, IJsonParser reader)
        {
            throw new InvalidDataException($"Expected property to have array type, but was: {state.CurrentTokenType}. Id: '{Id ?? "N/A"}'. Around: {reader.GenerateErrorState()}");
        }

        public void Dispose()
        {
            for (int i = _allocations.Count - 1; i >= 0; i--)
            {
                _ctx.ReturnMemory(_allocations[i]);
            }
            _allocations.Clear();

            _disposed = true;
        }

        public void Reset(JsonOperationContext ctx)
        {
            AssertNotDisposed();

            if (_ctx == null) // should never happen
            {
                _ctx = ctx;
                _metadataCollections = _ctx.GetLazyStringForFieldWithCaching(CollectionName.MetadataCollectionSegment);
                _metadataExpires = _ctx.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Expires);
                return;
            }
            Id = null;
            ChangeVector = null;
            Flags = DocumentFlags.None;
            NonPersistentFlags = NonPersistentDocumentFlags.None;
            SeenAttachments = false;
            SeenCounters = false;
            SeenTimeSeries = false;
            _depth = 0;
            _state = State.None;
            _readingMetadataObject = false;
            _ctx = ctx;
            _metadataCollections = _ctx.GetLazyStringForFieldWithCaching(CollectionName.MetadataCollectionSegment);
            _metadataExpires = _ctx.GetLazyStringForFieldWithCaching(Constants.Documents.Metadata.Expires);
        }

        [Conditional("DEBUG")]
        private void AssertNotDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(BlittableMetadataModifier));
        }
    }
}
