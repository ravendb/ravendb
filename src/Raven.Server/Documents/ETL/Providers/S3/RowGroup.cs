using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Parquet.Data;
using Raven.Server.Documents.ETL.Providers.SQL;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.S3
{
    public class RowGroup
    {
        public RowGroup(string name, string key)
        {
            TableName = name;
            PartitioningKey = key;
        }

        public string TableName { get; set; }

        public string PartitioningKey { get; set; }

        public Field[] Fields { get; private set; }

        public IList[] Data { get; private set; }

        public long Count { get; private set; }

        private const int MaxSize = 50_000;

        public bool Add(ToS3Item item)
        {
            if (Count == MaxSize)
                return false;

            var propCount = item.Properties.Count;

            if (Count == 0)
            {
                Fields = new Field[propCount];
                Data = new IList[propCount];

                for (var i = 0; i < propCount; i++)
                {
                    var prop = item.Properties[i];

                    var t = prop.Value.GetType();
                    // todo use prop.Type 
                    switch (prop.Type)
                    {
                        case BlittableJsonToken.StartObject:
                            break;
                        case BlittableJsonToken.StartArray:
                            break;
                        case BlittableJsonToken.Integer:
                            break;
                        case BlittableJsonToken.LazyNumber:
                            break;
                        case BlittableJsonToken.String:
                            break;
                        case BlittableJsonToken.CompressedString:
                            break;
                        case BlittableJsonToken.Boolean:
                            break;
                        case BlittableJsonToken.Null:
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    if (t == typeof(LazyStringValue))
                    {
                        Fields[i] = new DataField(prop.Id, typeof(string));
                        Data[i] = new List<string>();
                        continue;
                    }

                    Fields[i] = new DataField(prop.Id, t);
                    //Data[i] = Array.CreateInstance(t, MaxSize);

                    var listType = typeof(List<>).MakeGenericType(t);
                    Data[i] = (IList)Activator.CreateInstance(listType);
                }
            }

            Debug.Assert(Data != null, nameof(Data) + " != null");
            Debug.Assert(Data.Length == propCount, "Invalid Data");
            Debug.Assert(Fields.Length == propCount, "Invalid Fields info");


            for (var i = 0; i < propCount; i++)
            {
                var columnData = Data[i];
                var val = item.Properties[i].Value;
                var dataField = (DataField)Fields[i];

                /*                switch (dataField.DataType)
                                {
                                    case DataType.Unspecified:
                                        break;
                                    case DataType.Boolean:
                                        ((bool[])columnData)[Count] = (bool)val;
                                        continue;
                                    case DataType.Int32:
                                    case DataType.Int64:
                                        ((long[])columnData)[Count] = (long)val;
                                        continue;
                                    case DataType.String:
                                        ((string[])columnData)[Count] = val.ToString();
                                        continue;
                                    case DataType.Float:
                                    case DataType.Double:
                                        ((double[])columnData)[Count] = (double)val;
                                        continue;
                                }*/

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
                }

                throw new ArgumentOutOfRangeException();
            }

            Count++;

            return true;
        }
    }
}
