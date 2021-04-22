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
        public const string LastModifiedColumn = "_lastModifiedTicks";

        public Dictionary<string, DataField> Fields => _fields ??= GenerateDataFields();

        public override int Count => _count;

        private const int DefaultMaxItemsInGroup = 25 * 10;
        private RowGroup _group;
        private readonly Dictionary<string, DataType> _dataFields;
        private Dictionary<string, DataField> _fields;
        private readonly int _maxItemsPerGroup;
        private readonly string _tableName, _key;
        private string _documentIdColumn, _localPath, _fileName;
        private int _count;
        private bool[] _boolArr;
        private string[] _strArr;
        private double[] _doubleArr;
        private decimal[] _decimalArr;
        private long[] _longArr;
        private DateTimeOffset[] _dtoArr;
        private TimeSpan[] _tsArr;
        private readonly Logger _logger;
        private OlapEtlConfiguration _configuration;

        public ParquetTransformedItems(string name, string key, string tmpPath, string fileNamePrefix, OlapEtlConfiguration configuration, Logger logger) : base(OlapEtlFileFormat.Parquet)
        {
            _tableName = name;
            _key = key;
            _logger = logger;
            _maxItemsPerGroup = DefaultMaxItemsInGroup;
            _configuration = configuration;
            _dataFields = new Dictionary<string, DataType>();

            SetPath(tmpPath, fileNamePrefix);
        }

        private void SetPath(string tmpFilePath, string fileNamePrefix)
        {
            string idColumn = default;
            if (_configuration.OlapTables != null)
            {
                foreach (var olapTable in _configuration.OlapTables)
                {
                    if (olapTable.TableName != _tableName)
                        continue;

                    idColumn = olapTable.DocumentIdColumn;
                    break;
                }
            }

            _documentIdColumn = string.IsNullOrEmpty(idColumn)
                ? DefaultIdColumn
                : idColumn;

            _fileName = $"{fileNamePrefix}_{Guid.NewGuid()}.{Format}";
            _localPath = Path.Combine(tmpFilePath, _fileName);
        }


        public override string GenerateFileFromItems(out string folderName, out string fileName)
        {
            fileName = _fileName;
            folderName = _key;
            if (string.IsNullOrEmpty(_configuration.CustomPrefix) == false)
            {
                folderName = $"{_configuration.CustomPrefix}/{folderName}";
            }

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
                            array = _doubleArr ??= new double[data.Count];
                            break;
                        case DataType.Decimal:
                            array = _decimalArr ??= new decimal[data.Count];
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

                    Debug.Assert(array.Length == data.Count, $"Invalid field data on property '{kvp.Key}'");

                    data.CopyTo(array, 0);
                    groupWriter.WriteColumn(new DataColumn(field, array));
                }
            }

            _boolArr = null;
            _longArr = null;
            _strArr = null;
            _doubleArr = null;
            _decimalArr = null;
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
                        var lnv = (LazyNumberValue)prop.Value;
                        if (lnv.TryParseULong(out var ulongValue))
                        {
                            prop.Value = ulongValue;
                            propType = DataType.Int64;
                            data ??= new List<long>();
                        }
                        else if (lnv.TryParseDecimal(out var decimalValue))
                        {
                            prop.Value = decimalValue;
                            propType = DataType.Decimal;
                            data ??= new List<decimal>();
                        }
                        else
                        {
                            prop.Value = (double)lnv;
                            propType = DataType.Double;
                            data ??= new List<double>();
                        }
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

                else if (propType != dataType)
                {
                    // data type change

                    if ((dataType == DataType.Int64 || dataType == DataType.Decimal) && TryChangeDataType(propType, data, out data))
                    {
                        // change previous data from 'long' to 'double' / 'decimal'
                        // or from 'decimal' to 'double'
                        UpdateField(dataType = propType, propName, data, group, addDefaultData: false);
                    }

                    else if ((propType == DataType.Int64 || propType == DataType.Decimal) && TryChangeValueType(dataType, prop.Value, out var newValue))
                    {
                        // change current value from 'long' to 'double' / 'decimal'
                        // or from 'decimal' to 'double'
                        prop.Value = newValue;
                    }
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

        private static bool TryChangeDataType(DataType propType, IList data, out IList newData)
        {
            switch (propType)
            {
                case DataType.Double:
                {
                    newData = new List<double>();

                    foreach (var number in data)
                    {
                        var asDouble = Convert.ToDouble(number);

                        newData.Add(asDouble);
                    }

                    return true;
                }

                case DataType.Decimal:
                {
                    newData = new List<decimal>();

                    foreach (var number in data)
                    {
                        var asDecimal = Convert.ToDecimal(number);

                        newData.Add(asDecimal);
                    }

                    return true;
                }

                default:
                    newData = data;
                    return false;
            }
        }

        private static bool TryChangeValueType(DataType dataType, object value, out object newValue)
        {
            newValue = default;

            switch (dataType)
            {
                case DataType.Decimal:
                    newValue = Convert.ToDecimal(value);
                    return true;
                case DataType.Double:
                    newValue = Convert.ToDouble(value);
                    return true;
                default:
                    return false;
            }
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
                case DataType.String:
                    data?.Add(value.ToString());
                    break;
                case DataType.Int64:
                    data?.Add((long)value);
                    break;
                case DataType.Decimal:
                    data?.Add((decimal)value);
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

        private void UpdateField(DataType dataType, string propName, IList data, RowGroup group, bool addDefaultData = true)
        {
            _dataFields[propName] = dataType;
            group.Data[propName] = data;

            if (addDefaultData == false) 
                return;
            
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
                _logger.Info($"Inserted {_group.Count} records to '{_tableName}/{_key}' table " +
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
