using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Parquet;
using Parquet.Schema;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public sealed class ParquetTransformedItems : OlapTransformedItems
    {
        public const string DefaultIdColumn = "_id";
        public const string LastModifiedColumn = "_lastModifiedTime";

        public override int Count => _count;

        public Dictionary<string, DataField> Fields => _fields ??= GenerateDataFields();

        public RowGroup RowGroup => _group;

        public string Key => _key;

        private readonly RowGroup _group;
        private readonly Dictionary<string, Type> _dataTypes;
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
        private DateTime[] _dtArr;
        private int[] _timeMillisArr;

        private const string DateTimeFormat = "yyyy-MM-dd-HH-mm-ss.ffffff";
        private const string Extension = "parquet";

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
            _dataTypes = new Dictionary<string, Type>();
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

        public override string GenerateFile(out UploadInfo uploadInfo)
        {
            var nowAsString = DateTime.UtcNow.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

            uploadInfo = new UploadInfo
            {
                FileName = $"{nowAsString}-{_fileNameSuffix}.{Extension}",
                FolderName = _remoteFolderName,
            };

            var localPath = Path.Combine(_tmpFilePath, _localFolderName ?? string.Empty, uploadInfo.FileName);
            if (_localFolderName != null)
                Directory.CreateDirectory(Path.Combine(_tmpFilePath, _localFolderName));

            using (Stream fileStream = File.Open(localPath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                var parquetWriter = ParquetWriter.CreateAsync(new ParquetSchema(Fields.Values), fileStream).GetAwaiter().GetResult();
                try
                {
                    WriteGroup(parquetWriter);
                }
                finally
                {
                    // ParquetWriter is IAsyncDisposable only, disposing it writes the file footer
                    parquetWriter.DisposeAsync().GetAwaiter().GetResult();
                }
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
                if (kvp.Value == null)
                    continue;

                fields[kvp.Key] = CreateDataField(kvp.Key, kvp.Value);
            }

            fields[_documentIdColumn] = new DataField(_documentIdColumn, typeof(string));
            fields[LastModifiedColumn] = new DataField(LastModifiedColumn, typeof(long));

            return fields;
        }

        private static DataField CreateDataField(string name, Type dataType)
        {
            // TimeSpan is not a supported Parquet.Net type anymore, a TIME column with
            // millisecond precision is what earlier versions used to write for it
            if (dataType == typeof(TimeSpan))
                return new TimeDataField(name, TimeUnitPrecision.Millis);

            return new DataField(name, dataType);
        }

        private void WriteGroup(ParquetWriter parquetWriter)
        {
            AddMandatoryFields();

            using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
            {
                // columns must be written in the order they appear in the schema,
                // and the schema is generated from 'Fields', so we iterate over it
                foreach (var kvp in Fields)
                {
                    var field = kvp.Value;
                    var data = _group.Data[kvp.Key];

                    // 'ClrType' is the type Parquet stores the column as, which is not the type we
                    // asked for in a couple of cases, so those are matched on the field itself
                    if (field is TimeDataField)
                    {
                        WriteTimeColumn(groupWriter, field, data, ref _timeMillisArr);
                    }
                    else if (field.ClrType == typeof(ReadOnlyMemory<char>))
                    {
                        WriteStringColumn(groupWriter, field, data, ref _strArr);
                    }
                    else if (field.ClrType == typeof(bool))
                    {
                        WriteColumn(groupWriter, field, data, ref _boolArr);
                    }
                    else if (field.ClrType == typeof(byte))
                    {
                        WriteColumn(groupWriter, field, data, ref _byteArr);
                    }
                    else if (field.ClrType == typeof(sbyte))
                    {
                        WriteColumn(groupWriter, field, data, ref _sbyteArr);
                    }
                    else if (field.ClrType == typeof(short))
                    {
                        WriteColumn(groupWriter, field, data, ref _shortArr);
                    }
                    else if (field.ClrType == typeof(int))
                    {
                        WriteColumn(groupWriter, field, data, ref _intArr);
                    }
                    else if (field.ClrType == typeof(long))
                    {
                        WriteColumn(groupWriter, field, data, ref _longArr);
                    }
                    else if (field.ClrType == typeof(ushort))
                    {
                        WriteColumn(groupWriter, field, data, ref _ushortArr);
                    }
                    else if (field.ClrType == typeof(uint))
                    {
                        WriteColumn(groupWriter, field, data, ref _uintArr);
                    }
                    else if (field.ClrType == typeof(ulong))
                    {
                        WriteColumn(groupWriter, field, data, ref _ulongArr);
                    }
                    else if (field.ClrType == typeof(float))
                    {
                        WriteColumn(groupWriter, field, data, ref _floatArr);
                    }
                    else if (field.ClrType == typeof(double))
                    {
                        WriteColumn(groupWriter, field, data, ref _doubleArr);
                    }
                    else if (field.ClrType == typeof(decimal))
                    {
                        WriteColumn(groupWriter, field, data, ref _decimalArr);
                    }
                    else if (field.ClrType == typeof(DateTime))
                    {
                        WriteColumn(groupWriter, field, data, ref _dtArr);
                    }
                    else
                    {
                        ThrowUnsupportedDataType(field.ClrType);
                    }
                }
            }

            _boolArr = null;
            _strArr = null;
            _dtArr = null;
            _timeMillisArr = null;
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

        private static void WriteColumn<T>(ParquetRowGroupWriter groupWriter, DataField field, IList data, ref T[] array)
            where T : struct
        {
            array ??= new T[data.Count];

            Debug.Assert(array.Length == data.Count, $"Invalid field data on property '{field.Name}'");

            data.CopyTo(array, 0);
            groupWriter.WriteAsync(field, new ReadOnlyMemory<T>(array)).GetAwaiter().GetResult();
        }

        private static void WriteTimeColumn(ParquetRowGroupWriter groupWriter, DataField field, IList data, ref int[] array)
        {
            array ??= new int[data.Count];

            Debug.Assert(array.Length == data.Count, $"Invalid field data on property '{field.Name}'");

            for (int i = 0; i < data.Count; i++)
                array[i] = (int)((TimeSpan)data[i]).TotalMilliseconds;

            groupWriter.WriteAsync(field, new ReadOnlyMemory<int>(array)).GetAwaiter().GetResult();
        }

        private static void WriteStringColumn(ParquetRowGroupWriter groupWriter, DataField field, IList data, ref string[] array)
        {
            array ??= new string[data.Count];

            Debug.Assert(array.Length == data.Count, $"Invalid field data on property '{field.Name}'");

            data.CopyTo(array, 0);

            // strings are reference types, so they need the dedicated overload
            groupWriter.WriteAsync(field, array).GetAwaiter().GetResult();
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
                    UpdateField(null, propName, data, _group);
                else
                    AddDefaultData(data, dataType, 1);
                return;
            }

            var propType = GetPropertyDataType(docId, prop, ref data);

            if (newField)
            {
                UpdateField(dataType = propType, propName, data, _group);
            }

            else if (dataType == null)
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

        private static Type GetPropertyDataType(LazyStringValue docId, OlapColumn prop, ref IList data)
        {
            Type propType;
            switch (prop.Type & BlittableJsonReaderBase.TypesMask)
            {
                case BlittableJsonToken.Integer:
                    propType = typeof(long);
                    data ??= new List<long>();
                    break;
                case BlittableJsonToken.LazyNumber:
                    var lnv = (LazyNumberValue)prop.Value;
                    if (lnv.TryParseULong(out var ulongValue))
                    {
                        prop.Value = ulongValue;
                        propType = typeof(long);
                        data ??= new List<long>();
                    }
                    else if (lnv.TryParseDecimal(out var decimalValue))
                    {
                        prop.Value = decimalValue;
                        propType = typeof(decimal);
                        data ??= new List<decimal>();
                    }
                    else
                    {
                        prop.Value = (double)lnv;
                        propType = typeof(double);
                        data ??= new List<double>();
                    }
                    break;
                case BlittableJsonToken.CompressedString:
                case BlittableJsonToken.String:
                    var str = prop.Value.ToString();
                    if (TryParseDate(str, out var dto))
                    {
                        propType = typeof(DateTime);
                        data ??= new List<DateTime>();
                        prop.Value = dto;
                        break;
                    }
                    if (TryParseTimeSpan(str, out var ts))
                    {
                        propType = typeof(TimeSpan);
                        data ??= new List<TimeSpan>();
                        prop.Value = ts;
                        break;
                    }
                    propType = typeof(string);
                    data ??= new List<string>();
                    break;
                case BlittableJsonToken.Boolean:
                    propType = typeof(bool);
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

        private static Type GetTypeFromObject(LazyStringValue docId, OlapColumn prop, ref IList data)
        {
            using (var objectValue = (BlittableJsonReaderObject)prop.Value)
            {
                if (objectValue.Count < 2 || objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Type), out object dbType) == false ||
                    objectValue.TryGetMember(nameof(SqlDocumentTransformer.VarcharFunctionCall.Value), out object fieldValue) == false)
                {
                    prop.Value = objectValue.ToString();
                    data ??= new List<string>();
                    return typeof(string);
                }

                Type propType;
                object value;
                var type = (DbType)Enum.Parse(typeof(DbType), dbType.ToString(), ignoreCase: true);

                switch (type)
                {
                    case DbType.Byte:
                        value = Convert.ToByte(fieldValue);
                        propType = typeof(byte);
                        data ??= new List<byte>();
                        break;
                    case DbType.SByte:
                        value = Convert.ToSByte(fieldValue);
                        propType = typeof(sbyte);
                        data ??= new List<sbyte>();
                        break;
                    case DbType.Int16:
                        value = Convert.ToInt16(fieldValue);
                        propType = typeof(short);
                        data ??= new List<short>();
                        break;
                    case DbType.Int32:
                        value = Convert.ToInt32(fieldValue);
                        propType = typeof(int);
                        data ??= new List<int>();
                        break;
                    case DbType.Int64:
                        value = Convert.ToInt64(fieldValue);
                        propType = typeof(long);
                        data ??= new List<long>();
                        break;
                    case DbType.UInt16:
                        value = Convert.ToUInt16(fieldValue);
                        propType = typeof(ushort);
                        data ??= new List<ushort>();
                        break;
                    case DbType.UInt32:
                        value = Convert.ToUInt32(fieldValue);
                        propType = typeof(uint);
                        data ??= new List<uint>();
                        break;
                    case DbType.UInt64:
                        value = Convert.ToUInt64(fieldValue);
                        propType = typeof(ulong);
                        data ??= new List<ulong>();
                        break;
                    case DbType.Single:
                        value = Convert.ToSingle(fieldValue);
                        propType = typeof(float);
                        data ??= new List<float>();
                        break;
                    case DbType.Double:
                        value = Convert.ToDouble(fieldValue);
                        propType = typeof(double);
                        data ??= new List<double>();
                        break;
                    case DbType.Decimal:
                        value = Convert.ToDecimal(fieldValue);
                        propType = typeof(decimal);
                        data ??= new List<decimal>();
                        break;
                    default:
                        throw new NotSupportedException($"Unsupported type '{dbType}' in object '{prop.Name}', On document '{docId}'");
                }

                prop.Value = value;
                return propType;
            }
        }

        private static bool TryChangeDataType(Type dataType, Type propType, IList data, out IList newData)
        {
            newData = data;

            if (propType == typeof(double))
            {
                if (dataType == typeof(long) || dataType == typeof(decimal))
                {
                    newData = new List<double>();
                    foreach (var number in data)
                    {
                        newData.Add(Convert.ToDouble(number));
                    }

                    return true;
                }

                return false;
            }

            if (propType == typeof(decimal))
            {
                if (dataType == typeof(long))
                {
                    newData = new List<decimal>();
                    foreach (var number in data)
                    {
                        newData.Add(Convert.ToDecimal(number));
                    }

                    return true;
                }

                return false;
            }

            if (propType == typeof(decimal))
            {
                if (data.Count != 0)
                    return false;

                // edge case : data might have been initialized to List<long> / List<decimal>
                if (dataType == typeof(double))
                    newData = new List<double>();
                else if (dataType == typeof(decimal))
                    newData = new List<decimal>();

                // no need for field update
                return false;
            }

            return false;
        }

        private static bool TryChangeValueType(Type dataType, Type propType, object value, out object newValue)
        {
            newValue = default;

            if (propType != typeof(long) && propType != typeof(decimal))
                return false;

            if (dataType == typeof(decimal))
            {
                newValue = Convert.ToDecimal(value);
                return true;
            }

            if (dataType == typeof(double))
            {
                newValue = Convert.ToDouble(value);
                return true;
            }

            return false;
        }

        private static void AddNewValue(IList data, Type dataType, object value)
        {
            if (dataType is null)
            {
                // nothing to do
            }
            else if (dataType == typeof(bool))
            {
                data?.Add((bool)value);
            }
            else if (dataType == typeof(string))
            {
                data?.Add(value.ToString());
            }
            else if (dataType == typeof(byte))
            {
                data?.Add((byte)value);
            }
            else if (dataType == typeof(sbyte))
            {
                data?.Add((sbyte)value);
            }
            else if (dataType == typeof(short))
            {
                data?.Add((short)value);
            }
            else if (dataType == typeof(int))
            {
                data?.Add((int)value);
            }
            else if (dataType == typeof(long))
            {
                data?.Add((long)value);
            }
            else if (dataType == typeof(ushort))
            {
                data?.Add((ushort)value);
            }
            else if (dataType == typeof(uint))
            {
                data?.Add((uint)value);
            }
            else if (dataType == typeof(ulong))
            {
                data?.Add((ulong)value);
            }
            else if (dataType == typeof(float))
            {
                data?.Add((float)value);
            }
            else if (dataType == typeof(double))
            {
                data?.Add((double)value);
            }
            else if (dataType == typeof(decimal))
            {
                data?.Add((decimal)value);
            }
            else if (dataType == typeof(DateTime))
            {
                data?.Add((DateTime)value);
            }
            else if (dataType == typeof(TimeSpan))
            {
                data?.Add((TimeSpan)value);
            }
            else
            {
                ThrowUnsupportedDataType(dataType);
            }
        }

        [DoesNotReturn]
        private static void ThrowUnsupportedDataType(Type dataType)
        {
            throw new NotSupportedException($"Unsupported data type '{dataType.Name}'");
        }

        private void UpdateField(Type dataType, string propName, IList data, RowGroup group, bool addDefaultData = true)
        {
            _dataTypes[propName] = dataType;
            group.Data[propName] = data;

            if (addDefaultData == false)
                return;

            AddDefaultData(data, dataType, group.Count);
        }

        private static void AddDefaultData(IList data, Type type, long count)
        {
            if (count == 0 || data == null)
                return;

            if (type is null)
            {
                // nothing to do
            }
            else if (type == typeof(bool))
            {
                AddDefaultData<bool>(data, count);
            }
            else if (type == typeof(byte))
            {
                AddDefaultData<byte>(data, count);
            }
            else if (type == typeof(sbyte))
            {
                AddDefaultData<sbyte>(data, count);
            }
            else if (type == typeof(short))
            {
                AddDefaultData<short>(data, count);
            }
            else if (type == typeof(int))
            {
                AddDefaultData<int>(data, count);
            }
            else if (type == typeof(long))
            {
                AddDefaultData<long>(data, count);
            }
            else if (type == typeof(ushort))
            {
                AddDefaultData<ushort>(data, count);
            }
            else if (type == typeof(uint))
            {
                AddDefaultData<uint>(data, count);
            }
            else if (type == typeof(ulong))
            {
                AddDefaultData<ulong>(data, count);
            }
            else if (type == typeof(float))
            {
                AddDefaultData<float>(data, count);
            }
            else if (type == typeof(double))
            {
                AddDefaultData<double>(data, count);
            }
            else if (type == typeof(decimal))
            {
                AddDefaultData<decimal>(data, count);
            }
            else if (type == typeof(string))
            {
                AddDefaultData<string>(data, count);
            }
            else if (type == typeof(DateTime))
            {
                AddDefaultData<DateTime>(data, count);
            }
            else if (type == typeof(TimeSpan))
            {
                AddDefaultData<TimeSpan>(data, count);
            }
            else
            {
                ThrowUnsupportedDataType(type);
            }
        }

        private static void AddDefaultData<T>(IList data, long count)
        {
            for (int j = 0; j < count; j++)
            {
                data.Add(default(T));
            }
        }

        internal static unsafe bool TryParseDate(string str, out DateTime dt)
        {
            fixed (char* c = str)
            {
                var result = LazyStringParser.TryParseDateTime(c, str.Length, out dt, out var dto, properlyParseThreeDigitsMilliseconds: true);
                switch (result)
                {
                    case LazyStringParser.Result.DateTime:
                        break;
                    case LazyStringParser.Result.DateTimeOffset:
                        dt = dto.DateTime;
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
