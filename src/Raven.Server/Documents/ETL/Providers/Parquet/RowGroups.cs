using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Parquet.Data;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Parquet
{
    public class RowGroups
    {
        public RowGroups(string name, string key)
        {
            TableName = name;
            PartitionKey = key;
            Groups = new List<RowGroup>();
            Fields = new Dictionary<string, DataField>();
        }

        public string TableName { get; set; }

        public string PartitionKey { get; set; }

        public Dictionary<string, DataField> Fields { get; private set; }

        public List<RowGroup> Groups { get; }

        private const int MaxItemsPerGroup = 50_000;

        public void Add(ToParquetItem item)
        {
            var group = GetCurrentGroup();

            //Debug.Assert(Fields.Length == propCount, "Invalid Fields info");
            //Debug.Assert(group.Data?.Count == propCount, "Invalid group data");

            var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < item.Properties.Count; i++)
            {
                var prop = item.Properties[i];
                var propName = prop.Id;
                names.Add(propName);

                DataType type = DataType.Unspecified;

                var newField = Fields.TryGetValue(propName, out var df) == false;
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
                        data?.Add(default);
                        break;
                    case BlittableJsonToken.Integer:
                        type = DataType.Int64;
                        data ??= new List<long>();
                        break;
                    case BlittableJsonToken.LazyNumber:
                        // todo
                        type = DataType.Double;
                        data ??= new List<double>();
                        break;
                    case BlittableJsonToken.String:
                        type = DataType.String;
                        data ??= new List<string>();
                        break;
                    case BlittableJsonToken.Boolean:
                        type = DataType.Boolean;
                        data ??= new List<bool>();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (newField)
                {
                    df = new DataField(propName, type);
                    Fields.Add(propName, df);

                    AddDefaultData(data, type, group.Count);
                    group.Data.Add(propName, data);
                }

                var dataType = df.DataType;
                if (prop.Type == BlittableJsonToken.Null)
                {
                    AddDefaultData(data, dataType, 1);
                    continue;
                }

                if (data != null && dataType == DataType.Unspecified)
                {
                    // existing field that had no values, until now
                    // need to change the field type and add default values to field data

                    Fields[propName] = new DataField(propName, dataType = type);
                    AddDefaultData(data, dataType, group.Count);
                }

                switch (dataType)
                {
                    case DataType.Unspecified:
                        break;
                    case DataType.Boolean:
                        data?.Add((bool)prop.Value);
                        break;
                    case DataType.Int64:
                        data?.Add((long)prop.Value);
                        break;
                    case DataType.String:
                        data?.Add(prop.Value.ToString());
                        break;
                    case DataType.Double:
                        data?.Add((double)prop.Value);
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }

            foreach (var kvp in Fields)
            {
                if (names.Contains(kvp.Key)) 
                    continue;

                // handle doc with missing prop
                AddDefaultData(group.Data[kvp.Key], kvp.Value.DataType, 1);
            }

            group.Count++;
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

            foreach (var kvp in Fields)
            {
                IList data = null;
                switch (kvp.Value.DataType)
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
        }
        public Dictionary<string, IList> Data { get; set; }

        public long Count { get; internal set; }
    }
}
