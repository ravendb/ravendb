using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Parquet;
using Parquet.Data;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.SQL;
using Sparrow.Json;
using DataColumn = Parquet.Data.DataColumn;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class ParquetTransformedItems : OlapTransformedItems
    {
        public const string DefaultIdColumn = "_id";
        public const string LastModifiedColumn = "_lastModifiedTime";

        public override int Count => _count;

        public Dictionary<string, DataField> Fields => _fields ??= GenerateDataFields();

        public RowGroup RowGroup => _group;

        public string Key => _key;

        private readonly RowGroup _group;
        private readonly Dictionary<string, DataType> _dataTypes;
        private Dictionary<string, DataField> _fields;
        private readonly string _tableName, _key, _tmpFilePath, _fileNameSuffix;
        private string _documentIdColumn, _remoteFolderName, _localFolderName;
        private int _count;
        private readonly OlapEtlConfiguration _configuration;
        private bool[] _boolArr;
        private string[] _strArr;
        private byte[] _byteArr;
        private sbyte[] _sbyteArr;
        private short[] _shortArr;
        private int[] _intArr;
        private long[] _longArr;
        private ushort[] _ushortArr;
        private uint[] _uintArr;
        private ulong[] _ulongArr;
        private float[] _floatArr;
        private double[] _doubleArr;
        private decimal[] _decimalArr;
        private DateTimeOffset[] _dtoArr;
        private TimeSpan[] _tsArr;

        private const string DateTimeFormat = "yyyy-MM-dd-HH-mm-ss.ffffff";
        private const string Extension = "parquet";

        private static readonly HashSet<char> SpecialChars = new HashSet<char> { '&', '@', ':', ',', '$', '=', '+', '?', ';', ' ', '"', '^', '`', '>', '<', '{', '}', '[', ']', '#', '\'', '~', '|' };
        private const string EncodingFormat = "%{0:X2}";

        private static readonly HashSet<char> InvalidFileNameChars = Path.GetInvalidFileNameChars().ToHashSet();

        private static readonly long UnixEpochTicks = new DateTime(1970, 1, 1).Ticks;

        public ParquetTransformedItems(string name, string key, string tmpPath, string fileNameSuffix, List<string> partitions, OlapEtlConfiguration configuration) 
            : base(OlapEtlFileFormat.Parquet)
        {
            _tableName = name;
            _key = key;
            _configuration = configuration;
            _fileNameSuffix = fileNameSuffix;
            _tmpFilePath = tmpPath;
            _dataTypes = new Dictionary<string, DataType>();
            _group = new RowGroup();

            SetIdColumn();
            GetSafeFolderName(name, partitions);
        }

        private void GetSafeFolderName(string name, List<string> partitions)
        {
            if (partitions == null)
            {
                _remoteFolderName = name;
                if (_configuration.Connection.LocalSettings != null)
                    _localFolderName = name;

                return;
            }

            StringBuilder remoteFolderBuilder = new StringBuilder(_key.Length);
            StringBuilder localFolderBuilder = _configuration.Connection.LocalSettings != null
                ? new StringBuilder(_key.Length)
                : null;

            remoteFolderBuilder.Append(name);
            localFolderBuilder?.Append(name);

            foreach (var partition in partitions)
            {
                remoteFolderBuilder.Append('/');
                localFolderBuilder?.Append(Path.DirectorySeparatorChar);

                var safeRemoteName = GetSafeNameForRemoteDestination(partition);
                remoteFolderBuilder.Append(safeRemoteName);

                localFolderBuilder?.Append(GetSafeNameForFileSystem(partition));
            }

            _remoteFolderName = remoteFolderBuilder.ToString();
            _localFolderName = localFolderBuilder?.ToString();
        }

        private void SetIdColumn()
        {
            if (_configuration.OlapTables != null)
            {
                foreach (var olapTable in _configuration.OlapTables)
                {
                    if (olapTable.TableName != _tableName)
                        continue;

                    _documentIdColumn = olapTable.DocumentIdColumn;
                    break;
                }
            }

            _documentIdColumn ??= DefaultIdColumn;
        }

        public override string GenerateFileFromItems(out string folderName, out string fileName)
        {
            var nowAsString = DateTime.UtcNow.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

            fileName = $"{nowAsString}-{_fileNameSuffix}.{Extension}";
            folderName = _remoteFolderName;

            var localPath = Path.Combine(_tmpFilePath, _localFolderName ?? string.Empty, fileName);
            if (_localFolderName != null)
                Directory.CreateDirectory(Path.Combine(_tmpFilePath, _localFolderName));
            
            using (Stream fileStream = File.Open(localPath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            using (var parquetWriter = new ParquetWriter(new Schema(Fields.Values), fileStream))
            {
                WriteGroup(parquetWriter);
            }

            _count = _group.Count;
            _group.Clear();

            return localPath;
        }

        private Dictionary<string, DataField> GenerateDataFields()
        {
            var fields = new Dictionary<string, DataField>(_dataTypes.Count + 2);

            foreach (var kvp in _dataTypes)
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
                        case DataType.Byte:
                            array = _byteArr ??= new byte[data.Count];
                            break;
                        case DataType.SignedByte:
                            array = _sbyteArr ??= new sbyte[data.Count];
                            break;
                        case DataType.Short:
                            array = _shortArr ??= new short[data.Count];
                            break;
                        case DataType.Int32:
                            array = _intArr ??= new int[data.Count];
                            break;
                        case DataType.Int64:
                            array = _longArr ??= new long[data.Count];
                            break;
                        case DataType.UnsignedInt16:
                            array = _ushortArr ??= new ushort[data.Count];
                            break;
                        case DataType.UnsignedInt32:
                            array = _uintArr ??= new uint[data.Count];
                            break;
                        case DataType.UnsignedInt64:
                            array = _ulongArr ??= new ulong[data.Count];
                            break;
                        case DataType.Float:
                            array = _floatArr ??= new float[data.Count];
                            break;
                        case DataType.Double:
                            array = _doubleArr ??= new double[data.Count];
                            break;
                        case DataType.Decimal:
                            array = _decimalArr ??= new decimal[data.Count];
                            break;
                        case DataType.String:
                            array = _strArr ??= new string[data.Count];
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
            _strArr = null;
            _dtoArr = null;
            _tsArr = null;
            _byteArr = null;
            _sbyteArr = null;
            _shortArr = null;
            _intArr = null;
            _longArr = null;
            _ushortArr = null;
            _uintArr = null;
            _ulongArr = null;
            _floatArr = null;
            _doubleArr = null;
            _decimalArr = null;
        }

        internal void AddMandatoryFields()
        {
            _group.Data[_documentIdColumn] = _group.Ids;
            _group.Data[LastModifiedColumn] = _group.LastModified;
        }

        public override void AddItem(ToOlapItem item)
        {
            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            _group.Ids.Add(item.DocumentId);
            _group.LastModified.Add(UnixTimestampFromDateTime(item.Document.LastModified));
            
            foreach (var prop in item.Properties)
            {
                names.Add(prop.Name);
                AddProperty(item.DocumentId, prop);
            }

            foreach (var kvp in _dataTypes)
            {
                if (names.Contains(kvp.Key)) 
                    continue;

                // handle item with missing field
                AddDefaultData(_group.Data[kvp.Key], kvp.Value, 1);
            }

            _group.Count++;
        }

        private void AddProperty(LazyStringValue docId, OlapColumn prop)
        {
            var propName = prop.Name;

            var newField = _dataTypes.TryGetValue(propName, out var dataType) == false;
            _group.Data.TryGetValue(propName, out var data);

            if (prop.Type == BlittableJsonToken.Null)
            {
                if (newField)
                    UpdateField(DataType.Unspecified, propName, data, _group);
                else
                    AddDefaultData(data, dataType, 1);
                return;
            }

            var propType = GetPropertyDataType(docId, prop, ref data);

            if (newField)
            {
                UpdateField(dataType = propType, propName, data, _group);
            }

            else if (dataType == DataType.Unspecified)
            {
                // existing field that had no values, until now
                // need to change the field type and add default values to fields' data

                Debug.Assert(data.Count == 0, "Invalid data. Data type is 'Unspecified', but data.Count = " + data.Count);

                UpdateField(dataType = propType, propName, data, _group);
            }

            else if (propType != dataType)
            {
                // data type change

                if (TryChangeDataType(dataType, propType, data, out data))
                {
                    // change previous data from 'long' to 'double' / 'decimal'
                    // or from 'decimal' to 'double'
                    UpdateField(dataType = propType, propName, data, _group, addDefaultData: false);
                }

                else if (TryChangeValueType(dataType, propType, prop.Value, out var newValue))
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
                                                    $"On document '{docId}', property '{prop.Name}'", e);
            }
        }

        private static DataType GetPropertyDataType(LazyStringValue docId, OlapColumn prop, ref IList data)
        {
            DataType propType;
            switch (prop.Type & BlittableJsonReaderBase.TypesMask)
            {
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
                case BlittableJsonToken.StartObject:
                    propType = GetTypeFromObject(docId, prop, ref data);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported {nameof(BlittableJsonToken)} '{prop.Type}'. " +
                                                    $"On document '{docId}', property '{prop.Name}'");
            }

            return propType;
        }

        private static DataType GetTypeFromObject(LazyStringValue docId, OlapColumn prop, ref IList data)
        {
            using (var objectValue = (BlittableJsonReaderObject)prop.Value)
            {
                if (objectValue.Count < 2 || objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Type), out object dbType) == false ||
                    objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Value), out object fieldValue) == false)
                {
                    prop.Value = objectValue.ToString();
                    data ??= new List<string>();
                    return DataType.String;
                }

                DataType propType;
                object value;
                var type = (DbType)Enum.Parse(typeof(DbType), dbType.ToString(), ignoreCase: true);

                switch (type)
                {
                    case DbType.Byte:
                        value = Convert.ToByte(fieldValue);
                        propType = DataType.Byte;
                        data ??= new List<byte>();
                        break;
                    case DbType.SByte:
                        value = Convert.ToSByte(fieldValue);
                        propType = DataType.SignedByte;
                        data ??= new List<sbyte>();
                        break;
                    case DbType.Int16:
                        value = Convert.ToInt16(fieldValue);
                        propType = DataType.Short;
                        data ??= new List<short>();
                        break;
                    case DbType.Int32:
                        value = Convert.ToInt32(fieldValue);
                        propType = DataType.Int32;
                        data ??= new List<int>();
                        break;
                    case DbType.Int64:
                        value = Convert.ToInt64(fieldValue);
                        propType = DataType.Int64;
                        data ??= new List<long>();
                        break;
                    case DbType.UInt16:
                        value = Convert.ToUInt16(fieldValue);
                        propType = DataType.UnsignedInt16;
                        data ??= new List<ushort>();
                        break;
                    case DbType.UInt32:
                        value = Convert.ToUInt32(fieldValue);
                        propType = DataType.UnsignedInt32;
                        data ??= new List<uint>();
                        break;
                    case DbType.UInt64:
                        value = Convert.ToUInt64(fieldValue);
                        propType = DataType.UnsignedInt64;
                        data ??= new List<ulong>();
                        break;
                    case DbType.Single:
                        value = Convert.ToSingle(fieldValue);
                        propType = DataType.Float;
                        data ??= new List<float>();
                        break;
                    case DbType.Double:
                        value = Convert.ToDouble(fieldValue);
                        propType = DataType.Double;
                        data ??= new List<double>();
                        break;
                    case DbType.Decimal:
                        value = Convert.ToDecimal(fieldValue);
                        propType = DataType.Decimal;
                        data ??= new List<decimal>();
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported type '{dbType}' in object '{prop.Name}', On document '{docId}'");
                }

                prop.Value = value;
                return propType;
            }
        }

        private static bool TryChangeDataType(DataType dataType, DataType propType, IList data, out IList newData)
        {
            newData = data;

            switch (propType)
            {
                case DataType.Double:
                {
                    switch (dataType)
                    {
                        case DataType.Int64:
                        case DataType.Decimal:
                            newData = new List<double>();
                            foreach (var number in data)
                            {
                                newData.Add(Convert.ToDouble(number));
                            }
                            return true;
                        default:
                            return false;

                    }
                }

                case DataType.Decimal:
                {
                    switch (dataType)
                    {
                        case DataType.Int64:
                            newData = new List<decimal>();
                            foreach (var number in data)
                            {
                                newData.Add(Convert.ToDecimal(number));
                            }
                            return true;
                        default:
                            return false;
                    }
                }

                case DataType.Int64:
                {
                    if (data.Count != 0)
                        return false;

                    switch (dataType)
                    {
                        // edge case : data might have been initialized to List<long> / List<decimal>
                        case DataType.Double:
                            newData = new List<double>();
                            break;
                        case DataType.Decimal:
                            newData = new List<decimal>();
                            break;
                    }

                    // no need for field update
                    return false;
                }

                default:
                    return false;
            }
        }

        private static bool TryChangeValueType(DataType dataType, DataType propType, object value, out object newValue)
        {
            newValue = default;

            if (propType != DataType.Int64 && propType != DataType.Decimal)
                return false;

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
                case DataType.Byte:
                    data?.Add((byte)value);
                    break;
                case DataType.SignedByte:
                    data?.Add((sbyte)value);
                    break;
                case DataType.Short:
                    data?.Add((short)value);
                    break;
                case DataType.Int32:
                    data?.Add((int)value);
                    break;
                case DataType.Int64:
                    data?.Add((long)value);
                    break;
                case DataType.UnsignedInt16:
                    data?.Add((ushort)value);
                    break;
                case DataType.UnsignedInt32:
                    data?.Add((uint)value);
                    break;
                case DataType.UnsignedInt64:
                    data?.Add((ulong)value);
                    break;
                case DataType.Float:
                    data?.Add((float)value);
                    break;
                case DataType.Double:
                    data?.Add((double)value);
                    break;
                case DataType.Decimal:
                    data?.Add((decimal)value);
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
            _dataTypes[propName] = dataType;
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
                case DataType.Byte:
                    AddDefaultData<byte>(data, count);
                    break;
                case DataType.SignedByte:
                    AddDefaultData<sbyte>(data, count);
                    break;
                case DataType.Short:
                    AddDefaultData<short>(data, count);
                    break;
                case DataType.Int32:
                    AddDefaultData<int>(data, count);
                    break;
                case DataType.Int64:
                    AddDefaultData<long>(data, count);
                    break;
                case DataType.UnsignedInt16:
                    AddDefaultData<ushort>(data, count);
                    break;
                case DataType.UnsignedInt32:
                    AddDefaultData<uint>(data, count);
                    break;
                case DataType.UnsignedInt64:
                    AddDefaultData<ulong>(data, count);
                    break;
                case DataType.Float:
                    AddDefaultData<float>(data, count);
                    break;
                case DataType.Double:
                    AddDefaultData<double>(data, count);
                    break;
                case DataType.Decimal:
                    AddDefaultData<decimal>(data, count);
                    break;
                case DataType.String:
                    AddDefaultData<string>(data, count);
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

        private static void AddDefaultData<T>(IList data, long count)
        {
            for (int j = 0; j < count; j++)
            {
                data.Add(default(T));
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

        internal static string GetSafeNameForFileSystem(string name)
        {
            var sb = new StringBuilder(name.Length);
            foreach (var @char in name)
            {
                if (InvalidFileNameChars.Contains(@char))
                {
                    sb.Append('_');
                    continue;
                }

                sb.Append(@char);
            }

            return sb.ToString();
        }

        internal static string GetSafeNameForRemoteDestination(string str)
        {
            var builder = new StringBuilder(str.Length);
            foreach (char @char in str)
            {
                if (@char == '/')
                {
                    builder.Append('_');
                    continue;
                }

                if (SpecialChars.Contains(@char) || @char <= 31 || @char == 127)
                {
                    builder.AppendFormat(EncodingFormat, (int)@char);
                    continue;
                }

                builder.Append(@char);
            }

            return builder.ToString();
        }

        internal static long UnixTimestampFromDateTime(DateTime date)
        {
            long unixTimestamp = date.Ticks - UnixEpochTicks;
            unixTimestamp /= TimeSpan.TicksPerSecond;
            return unixTimestamp;
        }
    }
}
