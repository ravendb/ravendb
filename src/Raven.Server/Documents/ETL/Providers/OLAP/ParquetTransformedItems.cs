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
        public const string DefaultIdColumn = "_id";
        public const string DefaultPartitionColumn = "_dt";
        public const string LastModifiedColumn = "_lastModifiedTicks";

        public Dictionary<string, DataField> Fields => _fields ??= GenerateDataFields();

        public override int Count => _count;

        private static readonly string UrlEscapedEqualsSign = System.Net.WebUtility.UrlEncode("=");
        private const int DefaultMaxItemsInGroup = 50_000;
        private RowGroup _group;
        private readonly Dictionary<string, DataType> _dataFields;
        private Dictionary<string, DataField> _fields;
        private readonly int _maxItemsPerGroup;
        private readonly string _collectionName, _partitionKey;
        private string _documentIdColumn;
        private string _prefix;
        private string _localPath, _remotePath;
        private int _count;
        private bool[] _boolArr;
        private string[] _strArr;
        private double[] _doubleArr;
        private long[] _longArr;
        private DateTimeOffset[] _dtoArr;
        private TimeSpan[] _tsArr;
        private readonly Logger _logger;

        public ParquetTransformedItems(string name, string key, string tmpPath, string fileNamePrefix, OlapEtlConfiguration configuration, Logger logger) : base(OlapEtlFileFormat.Parquet)
        {
            _collectionName = name;
            _partitionKey = key;
            _logger = logger;
            _maxItemsPerGroup = configuration.MaxNumberOfItemsInRowGroup ?? DefaultMaxItemsInGroup;
            _dataFields = new Dictionary<string, DataType>();

            SetPrefixAndPath(configuration, tmpPath, fileNamePrefix);
        }

        private void SetPrefixAndPath(OlapEtlConfiguration configuration, string tmpFilePath, string fileNamePrefix)
        {
            string partitionColumn = default, idColumn = default;
            if (configuration.OlapTables != null)
            {
                foreach (var olapTable in configuration.OlapTables)
                {
                    if (olapTable.TableName != _collectionName)
                        continue;

                    partitionColumn = olapTable.PartitionColumn;
                    idColumn = olapTable.DocumentIdColumn;
                    break;
                }
            }

            if (string.IsNullOrEmpty(partitionColumn))
                partitionColumn = DefaultPartitionColumn;

            _documentIdColumn = string.IsNullOrEmpty(idColumn)
                ? DefaultIdColumn
                : idColumn;

            _prefix = $"{_collectionName}/{partitionColumn}{UrlEscapedEqualsSign}{_partitionKey}";

            var fileName = $"{fileNamePrefix}_{Guid.NewGuid()}.{Format}";

            _localPath = Path.Combine(tmpFilePath, fileName);
            _remotePath = $"{_prefix}/{fileName}";
        }


        public override string GenerateFileFromItems(out string remotePath)
        {
            remotePath = _remotePath;
            WriteToFile();
            _group.Clear();
            
            return _localPath;
        }

        private void WriteToFile()
        {
            var append = File.Exists(_localPath);
            using (Stream fileStream = File.Open(_localPath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            using (var parquetWriter = new ParquetWriter(new Schema(Fields.Values), fileStream, append: append))
            {
                WriteGroup(parquetWriter);
                LogStats();
                _count += _group.Count;
            }

        }

        private void SetPath(string tmpFilePath, string fileNamePrefix)
        {
            var fileName = $"{fileNamePrefix}_{Guid.NewGuid()}.{Format}";

            _localPath = Path.Combine(tmpFilePath, fileName);
            _remotePath = $"{_prefix}/{fileName}";
        }

        private Dictionary<string, DataField> GenerateDataFields()
        {
            var fields = new Dictionary<string, DataField>(_dataFields.Count + 2);

            foreach (var kvp in _dataFields)
            {
                if (kvp.Value == DataType.Unspecified)
                    continue;

                fields[kvp.Key] = new DataField(kvp.Key, kvp.Value);
            }

            fields[_documentIdColumn] = new DataField(_documentIdColumn, DataType.String);
            fields[LastModifiedColumn] = new DataField(LastModifiedColumn, DataType.Int64);

            return fields;
        }


        private void WriteGroup(ParquetWriter parquetWriter)
        {
            AddMandatoryFields();

            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
            {
                foreach (var kvp in _group.Data)
                {
                    if (Fields.TryGetValue(kvp.Key, out var field) == false)
                        continue;

                    var data = kvp.Value;
                    Array array;

                    switch (field.DataType)
                    {
                        case DataType.Boolean:
                            array = _boolArr ??= new bool[data.Count];
                            break;
                        case DataType.Int32:
                        case DataType.Int64:
                            array = _longArr ??= new long[data.Count];
                            break;
                        case DataType.String:
                            array = _strArr ??= new string[data.Count];
                            break;
                        case DataType.Float:
                        case DataType.Double:
                        case DataType.Decimal:
                            array = _doubleArr ??= new double[data.Count];
                            break;
                        case DataType.DateTimeOffset:
                            array = _dtoArr ??= new DateTimeOffset[data.Count];
                            break;
                        case DataType.TimeSpan:
                            array = _tsArr ??= new TimeSpan[data.Count];
                            break;
                        default:
                            ThrowUnsupportedDataType(field.DataType);
                            return;
                    }

                    data.CopyTo(array, 0);
                    groupWriter.WriteColumn(new DataColumn(field, array));
                }
            }

            _boolArr = null;
            _longArr = null;
            _strArr = null;
            _doubleArr = null;
            _dtoArr = null;
            _tsArr = null;
        }

        private void AddMandatoryFields()
        {
            _group.Data[_documentIdColumn] = _group.Ids;
            _group.Data[LastModifiedColumn] = _group.LastModified;
        }

        public override void AddItem(ToOlapItem item)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var group = GetCurrentGroup();
            
            group.Ids.Add(item.DocumentId);
            group.LastModified.Add(item.Document.LastModified.Ticks);
            
            foreach (var prop in item.Properties)
            {
                var propName = prop.Id;
                names.Add(propName);

                DataType propType = DataType.Unspecified;

                var newField = _dataFields.TryGetValue(propName, out var dataType) == false;
                group.Data.TryGetValue(propName, out var data);

                switch (prop.Type & BlittableJsonReaderBase.TypesMask)
                {
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
                        propType = DataType.Double;
                        data ??= new List<double>();
                        break;
                    case BlittableJsonToken.CompressedString:
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

                else if (dataType == DataType.Unspecified)
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
            if (_group == null || _group.Count == _maxItemsPerGroup)
                AddNewGroup();

            return _group;
        }

        private void AddNewGroup()
        {
            _group ??= new RowGroup();

            if (_group.Count == _maxItemsPerGroup)
            {
                WriteToFile();
                _group.Clear();
            }

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
                        return;
                }

                _group.Data.Add(kvp.Key, data);
            }
        }

        private static void AddDefaultData<T>(IList data, long count)
        {
            for (int j = 0; j < count; j++)
            {
                data.Add(default(T));
            }
        }

        private void LogStats()
        {
            if (_logger?.IsInfoEnabled ?? false)
            {
                _logger.Info($"Inserted {_group.Count} records to '{_collectionName}/{_partitionKey}' table " +
                            $"from the following documents: {string.Join(", ", _group.Ids)}");
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
