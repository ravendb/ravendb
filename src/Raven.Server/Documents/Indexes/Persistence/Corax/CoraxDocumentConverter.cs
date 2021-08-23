using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Corax;
using Raven.Client;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Json;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    //we don't need this class, but for compatibily purpose with lucene classes we will use it
    public sealed class CoraxDocumentConverter : IDisposable
    {
        private readonly BlittableJsonTraverser _blittableTraverser;
        private readonly Dictionary<string, IndexField> _fields;
        private readonly Index _index;
        private ByteStringContext _bsc;
        private readonly Dictionary<Slice, int> _knownFields;
        public CoraxDocumentConverter(
                Index index, 
                bool indexImplicitNull = false, 
                bool indexEmptyEntries = true, 
                string keyFieldName = null, 
                bool storeValue = false, 
                string storeValueFieldName = Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
            // : base(index, indexEmptyEntries, numberOfBaseFields: 1, keyFieldName, storeValue, storeValueFieldName)
        {
            
            _index = index ?? throw new ArgumentNullException(nameof(index));
            _index = index;
            var fields = index.Definition.IndexFields.Values;
            var dictionary = new Dictionary<string, IndexField>(fields.Count, default(OrdinalStringStructComparer));
            foreach (var field in fields)
                dictionary[field.Name] = field;
            _fields = dictionary;
            _blittableTraverser = storeValue ? BlittableJsonTraverser.FlatMapReduceResults : BlittableJsonTraverser.Default;
            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _knownFields = GetKnownFields();
        }

        public Dictionary<Slice, int> GetKnownFields()
        {
            if (_knownFields != null)
                return _knownFields;
            var knownFields = new Dictionary<Slice, int>();
            foreach (var field in _fields.Values)
            {
                if(field.Name == null) continue;
                
                Slice.From(_bsc, field.Name, ByteStringType.Immutable, out var value);
                knownFields.Add(value, field.Id);
            }

            return knownFields;
        }

        //public (ref IndexEntryWriter EntryWriter, Dictionary<Slice, int> Fields) GetDocumentForIndex()
        //{
        //    var entryWriter = new IndexEntryWriter(new byte[256], _knownFields);

        //    return (entryWriter, null);
        //}
        //TODO maciej: For now I will assigne 32Kb buffer, need to change it in future. BTW: There is a way to get max size of item from Document?
        public Span<byte> GetFields(LazyStringValue key, LazyStringValue sourceDocumentId, object doc, JsonOperationContext indexContext, out string id)
        {
            var document = (Document)doc;
            Span<byte> buffer = new byte[256];
            var entryWriter = new IndexEntryWriter(buffer, _knownFields);
            id = document.LowerId.ToLower();
            foreach (var indexField in _fields.Values)
            {
                if (BlittableJsonTraverserHelper.TryRead(_blittableTraverser, document, indexField.OriginalName ?? indexField.Name, out var value) == true)
                    GetRegularField(ref entryWriter, indexField, value, indexContext, out var shouldSkip);
            }

            entryWriter.Finish(out var output);
            return output;
        }

        //todo maciej: check how to optimalize changing it into bytes
        private void GetRegularField(ref IndexEntryWriter entryWriter, IndexField field, object value, JsonOperationContext indexContext, out bool shouldSkip,
            bool nestedArray = false)
        {
            var path = field.Name;
            var valueType = GetValueType(value);
            shouldSkip = false;
            if (valueType == ValueType.Double)
            {
                //LazyStringValue doubleAsString;
                var ldv = value as LazyNumberValue;
                if (ldv != null)
                {
                    if (LuceneDocumentConverter.TryToTrimTrailingZeros(ldv, indexContext, out var doubleAsString) == false)
                        doubleAsString = ldv.Inner;
                    var @long = (long)ldv;
                    var @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                    entryWriter.Write(field.Id, doubleAsString.AsSpan(), @long, @double);

                }
                else
                {
                    string s = null;
                    switch (value)
                    {
                        case double d:
                            s = d.ToString("G");
                            break;

                        case decimal dm:
                            s = dm.ToString("G");
                            break;

                        case float f:
                            s = f.ToString("G");
                            break;
                    }
                    var @long = (long)ldv;
                    var @double = ldv.ToDouble(CultureInfo.InvariantCulture);
                    entryWriter.Write(field.Id, Encoding.UTF8.GetBytes(s), @long, @double);
                }
            }
            if (valueType is ValueType.Numeric)
            {
                var lazyNumber = value as LazyNumberValue; 
                var @long = (long)lazyNumber;
                var @double = lazyNumber.ToDouble(CultureInfo.InvariantCulture);

                entryWriter.Write(field.Id, Encoding.UTF8.GetBytes(lazyNumber.ToString()), @long, @double);
                return;
            }
            if (valueType is ValueType.String or ValueType.LazyString)
            {
                var lazyString = value as LazyStringValue;
                entryWriter.Write(field.Id, lazyString.AsSpan());//  Encoding.UTF8.GetBytes(value.ToString()));
                return;
            }

            shouldSkip = true;
        }

        public void Dispose()
        {
            ///todo maciej: check this later
            //_index?.Dispose();
            _bsc?.Dispose();

        }

        private static ValueType GetValueType(object value)
        {
            if (value == null)
                return ValueType.Null;

            var lazyStringValue = value as LazyStringValue;
            if (lazyStringValue != null)
                return lazyStringValue.Size == 0 ? ValueType.EmptyString : ValueType.LazyString;

            var valueString = value as string;
            if (valueString != null)
                return valueString.Length == 0 ? ValueType.EmptyString : ValueType.String;

            if (value is LazyNumberValue || value is double || value is decimal || value is float)
                return ValueType.Double;

            if (value is char)
                return ValueType.String;

            if (IsNumber(value))
                return ValueType.Numeric;

            return ValueType.ConvertToJson;
        }

        private static bool IsNumber(object value)
        {
            return value is long
                   || value is decimal
                   || value is int
                   || value is byte
                   || value is short
                   || value is ushort
                   || value is uint
                   || value is sbyte
                   || value is ulong
                   || value is float
                   || value is double;
        }
        private enum ValueType
        {
            Null,
            EmptyString,
            String,
            LazyString,
            Double,
            Numeric,
            ConvertToJson
        }

    }
}
