using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax.Querying;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Json;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Impl;

namespace Corax.Utils;

/// <summary>
/// This struct allows to read the entry terms from storage
///
/// Entry terms are used to hold all the unique terms for an entry, and are required to handle:
/// * Deletion of records
/// * More like this
/// * Optimized query processing
/// * Spatial checks
/// * Etc, etc, etc
///
/// The underlying assumption is that you are either:
/// * Have a large number of terms, but rarely do something with them. For example, analyzed a large text field. That is the oddball (but not rare) scenario.
/// * Small number of terms (one per field, with a small number of fields)
///
/// For that reason, we make no attempt to try to optimize lookups. To read the entry terms, you must always iterate over the terms.
///
/// We aren't actually storing the terms themselves in the entry, but just the references to them.
/// We take advantage of the container metadata to be able to store just the term id, and then be able to know what the field for that term is, so we can save
/// additional space.
///
/// In general, a term can be a string, or numeric. For numerics, we keep the string representation, as well as int64 and double values, to be able to do
/// comparisons properly. 
///
/// The format for the entries are:
///
/// delta encoded varint64 - term id 
///     We get int64 back, which is the *encoded* term id.
///     terms ids are limited to 2^51 (which gives us a max index size of ~2 petabyte)
///
/// The bottom 3 bits are used to indicate what type of term this is:
///
///     0th bit - numeric long
///     1st bit - numeric double
///     2nd bit - term frequency > 1
///
/// We hav ethe following options, there for:
/// * 0b000 - Simple term, non numeric, term id (with cleared bottom bits is the container id for the term)
/// * 0b001 - Numeric term, where the term id equal to above, but we also have delta encoded zigzag int64 for the value. The double value is equal to the long value.
/// * 0b010 - See below #1
/// * 0b011 - Numeric term, term as above, long as above, but the double is *different* from the long, so we store is using the full 8 bytes.
/// * 0b100 - Simple term, non numeric, with a term frequency > 1, bits 3..11 are used to hold the term frequency, need to shift it to get the real term id
/// * 0b101 - Numeric term, zigzag delta encoded long with long & double are the same, with term frequency
/// * 0b110 - See below #2
/// * 0b111 - Numeric term, zigzag delta encoded long and long form double, with term frequency
///
///
/// Given the logic we have, there are two special values to deal with:
/// * 0b010 - invalid, since this says that we have a double, but not a long
/// * 0b110 - invalid, since this says that we have a double, but not a long, with a term freq > 1
///
/// We reuse those values for additional purposes:
///
/// * 0b010 - indicates a spatial point. The term id in this case holds the *field root page*, not a term. This is followed by two doubles (raw) representing lat/lng.
/// * 0b110 - indicates that this is a  *stored* field. The term id contains the type of term we deal with, not a term.
///
///
///### NULL Handling:
///We store the root page multiplied by PageSize (which results in 0b000 at the end).
///In cases where TermId is divisible by PageSize, we have a suspicion that we could have a NULL value.
///To ensure that, we pass a list with all possible root pages, since all lowest bits are already in use.
/// </summary>
public unsafe struct EntryTermsReader : IDisposable
{
    private readonly LowLevelTransaction _llt;
    private readonly HashSet<long> _nullTermsMarkers;
    private readonly long _dicId;
    private byte* _cur;
    private readonly byte* _end, _start;
    private long _prevTerm;
    private long _prevLong;

    public CompactKey Current;
    public long CurrentLong;
    public long FieldRootPage;
    public long TermId;
    public double CurrentDouble;
    public double Latitude;
    public double Longitude;
    public UnmanagedSpan? StoredField;
    public short Frequency;
    public bool HasNumeric;
    public bool IsRaw;
    public bool IsList;
    public bool IsNull;

    //rootPages has to be sorted.
    public EntryTermsReader(LowLevelTransaction llt, HashSet<long> nullTermsMarkers, byte* cur, int size, long dicId, CompactKey existingKey = null)
    {
        _llt = llt;
        _nullTermsMarkers = nullTermsMarkers;
        _start = _cur;
        _cur = cur;
        _start = cur;
        _dicId = dicId;
        _end = cur + size;
        _prevTerm = 0;
        _prevLong = 0;

        Current = existingKey ?? llt.AcquireCompactKey();
        Current.Initialize(llt);
    }

    public bool FindNextStored(long fieldRootPage)
    {
        IsNull = false;

        while (MoveNextStoredField())
        {
            if (FieldRootPage == fieldRootPage)
                return true;
        }
        return false;
    }
    public bool FindNext(long fieldRootPage)
    {
        while (MoveNext())
        {
            if (FieldRootPage == fieldRootPage)
                return true;
        }
        return false;
    }
    
    public bool FindNextSpatial(long fieldRootPage)
    {        
        IsNull = false;

        while (MoveNextSpatial())
        {
            if (FieldRootPage == fieldRootPage)
                return true;
        }
        return false;
    }

    public bool MoveNext()
    {
        Start:
        if (_cur >= _end)
            return false;

        IsNull = false;
        var termContainerId = VariableSizeEncoding.Read<long>(_cur, out var offset) + _prevTerm;
        _prevTerm = termContainerId;
        _cur += offset;
        
        if ((termContainerId & 0b11) == 0b10) // special term markers, rare
        {
            HandleSpecialTerm(termContainerId, skipStoredFieldLoad: true);
            goto Start; // we skip those unless requested explicitly
        }

        HandleRegularTerm(termContainerId);
        return true;
    }
    
    public bool MoveNextSpatial()
    {
        Start:
        if (_cur >= _end)
            return false;

        var termContainerId = VariableSizeEncoding.Read<long>(_cur, out var offset) + _prevTerm;
        _prevTerm = termContainerId;
        _cur += offset;

        if ((termContainerId & 0b11) != 0b10) // Regular term, need to skip
        {
            HandleRegularTerm(termContainerId);
            goto Start; 
        }

        HandleSpecialTerm(termContainerId, skipStoredFieldLoad: true);
        if((termContainerId & 0b110) == 0b110) // stored field, need to skip
            goto Start; 
        return true;
    }
    
        
    public bool MoveNextStoredField()
    {
        Start:
        if (_cur >= _end)
            return false;

        IsNull = false;
        var termContainerId = VariableSizeEncoding.Read<long>(_cur, out var offset) + _prevTerm;
        _prevTerm = termContainerId;
        _cur += offset;
        
        if ((termContainerId & 0b11) != 0b10) // Regular term, need to skip
        {
            HandleRegularTerm(termContainerId);
            goto Start; 
        }

        HandleSpecialTerm(termContainerId, skipStoredFieldLoad: false);
        if((termContainerId & 0b110) != 0b110) // spatial field, need to skip
            goto Start; 
        return true;
    }

    private void HandleRegularTerm(long termContainerId)
    {
        var hasFreq = (termContainerId & 0b100) != 0;
        if (hasFreq == false)
        {
            Frequency = 1;
            TermId = termContainerId & ~0b111; // clear the marker bits
        }
        else
        {
            Frequency = EntryIdEncodings.FrequencyReconstructionFromQuantization((byte)(termContainerId >> 3));
            TermId = (termContainerId >> 8) & ~0b111;
        }

        IsNull = _nullTermsMarkers.Contains(TermId);
        
        Container.Get(_llt, TermId, out var termItem);
        FieldRootPage = termItem.PageLevelMetadata;
        if (IsNull == false)
        {
            TermsReader.Set(Current, termItem, _dicId);
        }

        HasNumeric = (termContainerId & 0b1) != 0;
        if (HasNumeric)
        {
            CurrentLong = ZigZagEncoding.Decode<long>(_cur, out var offset) + _prevLong;
            _prevLong = CurrentLong;
            _cur += offset;

            if ((termContainerId & 0b10) == 0b10)
            {
                CurrentDouble = *(double*)_cur;
                _cur += sizeof(double);
            }
            else
            {
                CurrentDouble = CurrentLong;
            }
        }
    }

    void HandleSpecialTerm(long termContainerId, bool skipStoredFieldLoad)
    {
        IsList = false;
        IsRaw = false;
        HasNumeric = false;
        FieldRootPage = termContainerId >> 3;
        TermId = -1;
        var hasStoredField = (termContainerId & 0b100) != 0;
        if (hasStoredField)
        {
            // we cast to byte first because we want to drop the positions markers we use
            // to manage the stored fields order, see RegisterTerm in IndexWriter
            var type = (StoredFieldType)(byte)(termContainerId & ~0b111);
            var val =  ZigZagEncoding.Decode<long>(_cur, out var offset) + _prevLong;
            _cur += offset;
            _prevLong = val;
            if (skipStoredFieldLoad == false)
            {
                IsList = type.HasFlag(StoredFieldType.List);
                HasNumeric = type.HasFlag(StoredFieldType.Tuple);
                switch (type & ~StoredFieldType.Markers)
                {
                    case StoredFieldType.Null:
                        StoredField = null;
                        FieldRootPage = val;
                        break;
                    case StoredFieldType.Empty:
                        StoredField = new(null, 0);
                        FieldRootPage = val;
                        break;
                    case StoredFieldType.Term:
                        Container.Get(_llt, val, out var termItem);
                        TermId = val;
                        FieldRootPage = termItem.PageLevelMetadata;
                        StoredField = termItem.ToUnmanagedSpan();
                        break;
                    case StoredFieldType.Raw:
                        IsRaw = true;
                        goto case StoredFieldType.Term;
                    case StoredFieldType.Empty | StoredFieldType.Raw:
                        IsRaw = true;
                        goto case StoredFieldType.Empty;
                    case StoredFieldType.None:
                        System.Diagnostics.Debug.Assert(false, $"This should not happen, got None stored field, type is: {type}");
                        goto case StoredFieldType.Null;
                    default:
                        throw new ArgumentOutOfRangeException(type.ToString());
                }
            }
        }
        else
        {
            Latitude  = *(double*)_cur;
            Longitude = *(double*)(_cur + sizeof(double));

            _cur += sizeof(double) + sizeof(double);
        }
    }

    public void Reset()
    {
        _cur = _start;
        _prevLong = 0;
        _prevTerm = 0;
        IsNull = false;
    }

    public string Debug(Indexing.IndexWriter w)
    {
        return Debug(w.GetIndexedFieldNamesByRootPage());
    }

    public string Debug(Querying.IndexSearcher r)
    {
        return Debug(r.GetIndexedFieldNamesByRootPage().ToDictionary(pair => pair.Key, pair => pair.Value.ToString()));
    }
    
    public string Debug(Dictionary<long, string> fields = null)
    {
        var sb = new StringBuilder();
        
        Reset();

        while (MoveNext())
        {
            if (fields?.TryGetValue(FieldRootPage, out var name) == true)
            {
                sb.Append(name);
            }
            else
            {
                sb.Append(FieldRootPage);
            }
            sb.Append(" - ").Append(Current);
            if (Frequency > 1)
            {
                sb.Append(" x").Append(Frequency);
            }

            if (HasNumeric)
            {
                sb.Append(" - long ").Append(CurrentLong).Append(" - double ").Append(CurrentDouble);
            }

            sb.AppendLine();
        } 
        Reset();
        while (MoveNextSpatial())
        {
            sb.Append("spatial: ");
            if (fields?.TryGetValue(FieldRootPage, out var name) == true)
            {
                sb.Append(name);
            }
            else
            {
                sb.Append(FieldRootPage);
            }

            sb.Append("Lat: ")
                .Append(Latitude)
                .Append("Lng: ")
                .Append(Longitude)
                .AppendLine();
        }
        Reset();
        while (MoveNextStoredField())
        {
            sb.Append("Stored: ");
            if (fields?.TryGetValue(FieldRootPage, out var name) == true)
            {
                sb.Append(name);
            }
            else
            {
                sb.Append(FieldRootPage);
            }
            if (StoredField == null)
            {
                sb.Append(" null value").AppendLine();
                continue;
            }

            if (IsRaw)
            {
                using var ctx = JsonOperationContext.ShortTermSingleUse();
                var json = new BlittableJsonReaderObject(StoredField.Value.Address, StoredField.Value.Length, ctx);
                sb.Append(' ').Append(json).AppendLine();
            }
            else
            {
                sb.Append(" '")
                    .Append(StoredField.Value.ToStringValue())
                    .Append('\'')
                    .AppendLine();
            }
        }

        return sb.ToString();
    }

    public void Dispose()
    {
        _llt.ReleaseCompactKey(ref Current);
    }
}

[Flags]
public enum StoredFieldType : byte
{
    None = 0,
    // Values
    Null = 1 << 3,
    Empty = 2 << 3,
    Term = 3 << 3,
    Raw = 4 << 3, 
    
    // Flag markers
    Tuple = 8 << 3,
    List = 16 << 3,
    
    Markers = Tuple | List,
}
