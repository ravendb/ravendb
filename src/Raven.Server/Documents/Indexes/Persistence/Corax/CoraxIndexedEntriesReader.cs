using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Amqp.Framing;
using Corax;
using Corax.Pipeline;
using Corax.Utils;
using Esprima.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public unsafe class CoraxIndexedEntriesReader : IDisposable
{
    private readonly JsonOperationContext _ctx;
    private readonly IndexSearcher _indexSearcher;


    public CoraxIndexedEntriesReader(JsonOperationContext ctx,IndexSearcher indexSearcher)
    {
        _ctx = ctx;
        _indexSearcher = indexSearcher;
    }

    public DynamicJsonValue GetDocument(ref EntryTermsReader entryReader)
    {
        var doc = new Dictionary<string, object>();
        HashSet<string> spatialSeenFields = null; 
        entryReader.Reset();
        while (entryReader.MoveNext())
        {
            if(_indexSearcher.FieldCache.TryGetField(entryReader.FieldRootPage, out var fieldName)==false)
                continue;

            string value = entryReader.Current.ToString();
            SetValue(fieldName, value);
        }
        entryReader.Reset();
        while (entryReader.MoveNextSpatial())
        {
            if(_indexSearcher.FieldCache.TryGetField(entryReader.FieldRootPage, out var fieldName)==false)
                continue;
            spatialSeenFields ??= new();
            if (spatialSeenFields.Add(fieldName))
            {
                doc.Remove(fieldName, out var geo); // move the geo-hashes to the side
                doc[fieldName+" [geo hashes]"] = geo;
            }
            SetValue(fieldName, new DynamicJsonValue
            {
                [nameof(entryReader.Latitude)] = entryReader.Latitude,
                [nameof(entryReader.Longitude)] = entryReader.Longitude,
            });
        }
        
        entryReader.Reset();
        while (entryReader.MoveNextStoredField())
        {
            if(_indexSearcher.FieldCache.TryGetField(entryReader.FieldRootPage, out var fieldName)==false)
                continue;

            if (entryReader.StoredField == null)
            {
                SetValue(fieldName, null);
                continue;
            }

            UnmanagedSpan span = entryReader.StoredField.Value;
            if (entryReader.IsList)
            {
                ForceList(fieldName);
            }
            
            if (entryReader.HasNumeric)
            {
                if (Utf8Parser.TryParse(span.ToReadOnlySpan(), out double d, out var consumed) && consumed == span.Length)
                {
                    SetValue(fieldName, d);
                }
                else
                {
                    SetValue(fieldName, Encoding.UTF8.GetString(span.ToReadOnlySpan()));
                }
            }
            else if (entryReader.IsRaw)
            {
                SetValue(fieldName, new BlittableJsonReaderObject(span.Address, span.Length, _ctx));
            }
            else
            {
                SetValue(fieldName, Encoding.UTF8.GetString(span.ToReadOnlySpan()));
            }
        }

        return ToJson();

        DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue();
            foreach (var (k,v) in doc)
            {
                json[k] = v;
            }

            return json;
        }

        void ForceList(string name)
        {
            if (doc.TryGetValue(name, out var existing) == false)
            {
                doc[name] = new List<object>();
            }

            if (existing is List<object>)
                return;
            doc[name] = new List<object> { existing };
        }

        void SetValue(string name, object value)
        {
            if (doc.TryGetValue(name, out var existing))
            {
                if (existing is List<object> l)
                {
                    l.Add(value);
                }
                else
                {
                    doc[name] = new List<object> { existing, value };
                }
            }
            else
            {
                doc[name] = value;
            }
        }
    }

    public void Dispose()
    {
    }
}
