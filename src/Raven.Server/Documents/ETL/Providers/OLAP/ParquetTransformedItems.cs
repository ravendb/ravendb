using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class ParquetTransformedItems : OlapTransformedItems
    {
        public ParquetTransformedItems(string name, string key) : base(OlapEtlFileFormat.Parquet)
        {
            CollectionName = name;
            PartitionKey = key;
            Groups = new List<RowGroup>();

            _dataFields = new Dictionary<string, DataType>();
        }

        public override string Prefix => PartitionKey;

        public string CollectionName { get; set; }

        public string PartitionKey { get; set; }

        public Dictionary<string, DataField> Fields => _fields ??= GenerateDataFields();

        public List<RowGroup> Groups { get; }

        private readonly Dictionary<string, DataType> _dataFields;

        private Dictionary<string, DataField> _fields;

        private const int MaxItemsPerGroup = 50_000;

        private Dictionary<string, DataField> GenerateDataFields()
        {
            var fields = new Dictionary<string, DataField>(_dataFields.Count);

            foreach (var kvp in _dataFields)
            {
                if (kvp.Value == DataType.Unspecified)
                    continue;

                fields[kvp.Key] = new DataField(kvp.Key, kvp.Value);
            }

            return fields;
        }

        public override int GenerateFileFromItems(string path, Logger logger = null)
        {
            var count = 0;

            using (Stream fileStream = File.OpenWrite(path))
            {
                using (var parquetWriter = new ParquetWriter(new Schema(Fields.Values), fileStream))
                {
                    foreach (var group in Groups)
                    {
                        WriteGroup(parquetWriter, group);
                        LogStats(group, logger);
                        count += group.Count;
                    }
                }
            }

            return count;
        }

        private void WriteGroup(ParquetWriter parquetWriter, RowGroup group)
        {
            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
            {
                foreach (var kvp in group.Data)
                {
                    if (Fields.TryGetValue(kvp.Key, out var field) == false)
                        continue;

                    var data = kvp.Value;
                    Array array = default;

                    switch (field.DataType)
                    {
                        case DataType.Unspecified:
                            break;
                        case DataType.Boolean:
                            array = ((List<bool>)data).ToArray();
                            break;
                        case DataType.Int32:
                        case DataType.Int64:
                            array = ((List<long>)data).ToArray();
                            break;
                        case DataType.String:
                            array = ((List<string>)data).ToArray();
                            break;
                        case DataType.Float:
                        case DataType.Double:
                        case DataType.Decimal:
                            array = ((List<double>)data).ToArray();
                            break;
                        case DataType.DateTimeOffset:
                            array = ((List<DateTimeOffset>)data).ToArray();
                            break;
                        case DataType.TimeSpan:
                            array = ((List<TimeSpan>)data).ToArray();
                            break;
                        default:
                            ThrowUnsupportedDataType(field.DataType);
                            return;
                    }

                    groupWriter.WriteColumn(new DataColumn(field, array));
                }
            }
        }

        public override void AddItem(ToOlapItem item)
        {
            var group = GetCurrentGroup();
            group.Ids.Add(item.DocumentId);
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var prop in item.Properties)
            {
                var propName = prop.Id;
                names.Add(propName);

                DataType propType = DataType.Unspecified;

                var newField = _dataFields.TryGetValue(propName, out var dataType) == false;
                group.Data.TryGetValue(propName, out var data);

                switch (prop.Type)
                {
                    case BlittableJsonToken.StartObject:
                    case BlittableJsonToken.StartArray:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.EmbeddedBlittable:
                        // todo
                        break;
                    case BlittableJsonToken.Null:
                        if (newField)
                            UpdateField(propType, propName, data, group);
                        else
                            AddDefaultData(data, dataType, 1);
                        continue;
                    case BlittableJsonToken.Integer:
                        propType = DataType.Int64;
                        data ??= new List<long>();
                        break;
                    case BlittableJsonToken.LazyNumber:
                        // todo
                        propType = DataType.Double;
                        data ??= new List<double>();
                        break;
                    case BlittableJsonToken.String:
                        var str = prop.Value.ToString();

                        if (TryParseDate(str, out var dto))
                        {
                            propType = DataType.DateTimeOffset;
                            data ??= new List<DateTimeOffset>();
                            prop.Value = dto;
                            break;
                        }

                        if (TryParseTimeSpan(str, out var ts))
                        {
                            propType = DataType.TimeSpan;
                            data ??= new List<TimeSpan>();
                            prop.Value = ts;
                            break;
                        }

                        propType = DataType.String;
                        data ??= new List<string>();
                        break;
                    case BlittableJsonToken.Boolean:
                        propType = DataType.Boolean;
                        data ??= new List<bool>();
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported {nameof(BlittableJsonToken)} '{prop.Type}'. " +
                                                        $"On document '{item.DocumentId}', property '{prop.Id}'");
                }

                if (newField)
                {
                    UpdateField(dataType = propType, propName, data, group);
                }

                else if (data != null && dataType == DataType.Unspecified)
                {
                    // existing field that had no values, until now
                    // need to change the field type and add default values to fields' data
                    
                    Debug.Assert(data.Count == 0, "Invalid data. Data type is 'Unspecified', but data.Count = " + data.Count);

                    UpdateField(dataType = propType, propName, data, group);
                }

                try
                {
                    AddNewValue(data, dataType, prop.Value);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to add value '{prop.Value}' to DataField of type '{dataType}'. " +
                                                        $"On document '{item.DocumentId}', property '{prop.Id}'", e);
                }

            }


            foreach (var kvp in _dataFields)
            {
                if (names.Contains(kvp.Key)) 
                    continue;

                // handle item with missing field
                AddDefaultData(group.Data[kvp.Key], kvp.Value, 1);
            }

            group.Count++;
        }

        private static void AddNewValue(IList data, DataType dataType, object value)
        {
            switch (dataType)
            {
                case DataType.Unspecified:
                    break;
                case DataType.Boolean:
                    data?.Add((bool)value);
                    break;
                case DataType.Int64:
                    data?.Add((long)value);
                    break;
                case DataType.String:
                    data?.Add(value.ToString());
                    break;
                case DataType.Double:
                    data?.Add((double)value);
                    break;
                case DataType.DateTimeOffset:
                    data?.Add((DateTimeOffset)value);
                    break;
                case DataType.TimeSpan:
                    data?.Add((TimeSpan)value);
                    break;
                default:
                    ThrowUnsupportedDataType(dataType);
                    break;
            }
        }

        private static void ThrowUnsupportedDataType(DataType dataType)
        {
            throw new NotSupportedException($"Unsupported {nameof(DataType)} '{dataType}'");
        }

        private void UpdateField(DataType dataType, string propName, IList data, RowGroup group)
        {
            _dataFields[propName] = dataType;
            group.Data[propName] = data;

            AddDefaultData(data, dataType, group.Count);
        }

        private static void AddDefaultData(IList data, DataType type, long count)
        {
            if (count == 0 || data == null)
                return;

            switch (type)
            {
                case DataType.Unspecified:
                    break;
                case DataType.Boolean:
                    AddDefaultData<bool>(data, count);
                    break;
                case DataType.Int64:
                    AddDefaultData<long>(data, count);
                    break;
                case DataType.String:
                    AddDefaultData<string>(data, count);
                    break;
                case DataType.Double:
                    AddDefaultData<double>(data, count);
                    break;
                case DataType.DateTimeOffset:
                    AddDefaultData<DateTimeOffset>(data, count);
                    break;
                case DataType.TimeSpan:
                    AddDefaultData<TimeSpan>(data, count);
                    break;
                default:
                    ThrowUnsupportedDataType(type);
                    break;
            }
        }

        private RowGroup GetCurrentGroup()
        {
            if (Groups.Count == 0 || Groups[^1].Count == MaxItemsPerGroup)
                return AddNewGroup();

            return Groups[^1];
        }

        private RowGroup AddNewGroup()
        {
            var group = new RowGroup();

            foreach (var kvp in _dataFields)
            {
                IList data = null;
                switch (kvp.Value)
                {
                    case DataType.Unspecified:
                        break;
                    case DataType.Boolean:
                        data = new List<bool>();
                        break;
                    case DataType.Int64:
                        data = new List<long>();
                        break;
                    case DataType.String:
                        data = new List<string>();
                        break;
                    case DataType.Double:
                        data = new List<double>();
                        break;
                    case DataType.DateTimeOffset:
                        data = new List<DateTimeOffset>();
                        break;
                    case DataType.TimeSpan:
                        data = new List<TimeSpan>();
                        break;
                    default:
                        ThrowUnsupportedDataType(kvp.Value);
                        return null;
                }

                group.Data.Add(kvp.Key, data);
            }

            Groups.Add(group);

            return group;
        }

        private static void AddDefaultData<T>(IList data, long count)
        {
            for (int j = 0; j < count; j++)
            {
                data.Add(default(T));
            }
        }

        private void LogStats(RowGroup group, Logger logger)
        {
            if (logger?.IsInfoEnabled ?? false)
            {
                logger.Info($"Inserted {group.Count} records to '{CollectionName}/{PartitionKey}' table " +
                            $"from the following documents: {string.Join(", ", group.Ids)}");
            }
        }

        private static unsafe bool TryParseDate(string str, out DateTimeOffset dto)
        {
            fixed (char* c = str)
            {
                var result = LazyStringParser.TryParseDateTime(c, str.Length, out var dt, out dto);
                switch (result)
                {
                    case LazyStringParser.Result.DateTime:
                        dto = dt;
                        break;
                    case LazyStringParser.Result.DateTimeOffset:
                        break;
                    case LazyStringParser.Result.Failed:
                        return false;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(LazyStringParser.Result), result, "Unknown result type");
                }

                return true;
            }
        }

        private static unsafe bool TryParseTimeSpan(string str, out TimeSpan ts)
        {
            fixed (char* c = str)
            {
                return LazyStringParser.TryParseTimeSpan(c, str.Length, out ts);
            }
        }
    }
}
