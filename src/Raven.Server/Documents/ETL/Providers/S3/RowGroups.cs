using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Parquet.Data;
using Raven.Server.Documents.ETL.Providers.SQL;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    public class RowGroups
    {
        public RowGroups(string name, string key)
        {
            TableName = name;
            PartitionKey = key;
            Groups = new List<RowGroup>();
        }

        public string TableName { get; set; }

        public string PartitionKey { get; set; }

        public Field[] Fields { get; private set; }

        public List<RowGroup> Groups { get; }

        private const int MaxItemsPerGroup = 50_000;

        public void Add(ToS3Item item)
        {
            var group = GetCurrentGroup(item);
            var propCount = item.Properties.Count;

            Debug.Assert(Fields.Length == propCount, "Invalid Fields info");
            Debug.Assert(group.Data?.Length == propCount, "Invalid group data");


            for (var i = 0; i < propCount; i++)
            {
                // ReSharper disable once PossibleNullReferenceException
                var columnData = group.Data[i];
                var val = item.Properties[i].Value;
                var dataField = (DataField)Fields[i];

                switch (dataField.DataType)
                {
                    case DataType.Unspecified:
                        break;
                    case DataType.Boolean:
                        ((List<bool>)columnData).Add((bool)val);
                        continue;
                    case DataType.Int32:
                    case DataType.Int64:
                        ((List<long>)columnData).Add((long)val);
                        continue;
                    case DataType.String:
                        ((List<string>)columnData).Add(val.ToString());
                        continue;
                    case DataType.Float:
                    case DataType.Double:
                        ((List<double>)columnData).Add((double)val);
                        continue;
                    default:
                        // todo handle other types, throw better exception
                        throw new ArgumentOutOfRangeException();
                }
            }

            group.Count++;
        }

        private RowGroup GetCurrentGroup(ToS3Item item)
        {
            if (Groups.Count == 0 || Groups[^1].Count == MaxItemsPerGroup)
                return AddNewGroup(item);

            return Groups[^1];
        }

        private RowGroup AddNewGroup(ToS3Item item)
        {
            var propCount = item.Properties.Count;

            Fields ??= new Field[propCount];

            var dataArray = new IList[propCount];

            for (var i = 0; i < propCount; i++)
            {
                var prop = item.Properties[i];

                Type type = null;
                IList data = default;
                switch (prop.Type)
                {
                    case BlittableJsonToken.StartObject:
                    case BlittableJsonToken.StartArray:
                    case BlittableJsonToken.CompressedString:
                    case BlittableJsonToken.Null:
                    case BlittableJsonToken.EmbeddedBlittable:
                        // todo
                        break;
                    case BlittableJsonToken.Integer:
                        type = typeof(long);
                        data = new List<long>();
                        break;
                    case BlittableJsonToken.LazyNumber:
                        // todo
                        type = typeof(double);
                        data = new List<double>();
                        break;
                    case BlittableJsonToken.String:
                        type = typeof(string);
                        data = new List<string>();
                        break;
                    case BlittableJsonToken.Boolean:
                        type = typeof(bool);
                        data = new List<bool>();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                Fields[i] ??= new DataField(prop.Id, type);

                dataArray[i] = data;
            }

            var group = new RowGroup(dataArray);
            Groups.Add(group);

            return group;
        }
    }

    public class RowGroup
    {
        public RowGroup(IList[] data)
        {
            Data = data;
        }

        public IList[] Data { get; }

        public long Count { get; internal set; }

    }
}
