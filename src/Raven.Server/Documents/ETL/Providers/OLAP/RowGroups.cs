using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Parquet.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.OLAP
{
    public class RowGroups
    {
        public RowGroups(string name, string key)
        {
            TableName = name;
            PartitionKey = key;
            Groups = new List<RowGroup>();
            _dataFields = new Dictionary<string, DataType>();
        }

        public string TableName { get; set; }

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

        public void Add(ToOlapItem item)
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
                        propType = DataType.String;
                        data ??= new List<string>();
                        break;
                    case BlittableJsonToken.Boolean:
                        propType = DataType.Boolean;
                        data ??= new List<bool>();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
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

                AddNewValue(data, dataType, prop.Value);
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
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private void UpdateField(DataType dataType, string propName, IList data, RowGroup group)
        {
            _dataFields[propName] = dataType;
            group.Data[propName] = data;

            AddDefaultData(data, dataType, group.Count);
        }

        private void AddDefaultData(IList data, DataType type, long count)
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

                default:
                    throw new ArgumentOutOfRangeException();
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

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                group.Data.Add(kvp.Key, data);
            }

            Groups.Add(group);

            return group;
        }

        private void AddDefaultData<T>(IList data, long count)
        {
            for (int j = 0; j < count; j++)
            {
                data.Add(default(T));
            }
        }
    }

    public class RowGroup
    {
        public RowGroup()
        {
            Data = new Dictionary<string, IList>();
            Ids = new List<string>();
        }
        public Dictionary<string, IList> Data { get; set; }

        public List<string> Ids { get; }

        public int Count { get; internal set; }
    }
}
