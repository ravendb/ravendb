using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Blittable.BlittableJsonWriterTests
{
    public class ManualBuilderTests : NoDisposalNeeded
    {
        [Fact]
        public void BasicObject()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();

                    builder.WritePropertyName("Age");

                    builder.WriteValue(1);
                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    using (var reader = builder.CreateReader())
                        Assert.Equal(1, reader.Count);
                }
            }
        }


        [Fact]
        public void BasicEmptyObject()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();
                    Assert.Equal(0, reader.Count);
                }
            }
        }

        [Fact]
        public void BasicNestedEmptyObject()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("EmptyObject");
                        {
                            builder.StartWriteObject();
                            builder.WriteObjectEnd();
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    using (var reader = builder.CreateReader())
                    {
                        Assert.Equal(1, reader.Count);
                        var nested = reader["EmptyObject"] as BlittableJsonReaderObject;
                        Assert.Equal(0, nested.Count);
                    }
                }
            }
        }

        [Fact]
        public void BasicIntFlatStructure()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Volume");
                        {
                            builder.WriteValue(44);
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(2, reader.Count);
                    var volumeValue = reader["Volume"].ToString();
                    Assert.Equal(44, int.Parse(volumeValue, CultureInfo.InvariantCulture));
                    var heightValue = reader["Height"].ToString();
                    Assert.Equal(55, int.Parse(heightValue, CultureInfo.InvariantCulture));
                }
            }
        }


        [Fact]
        public void BasicIntNestedStructure()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("Data");
                        {
                            builder.StartWriteObject();
                            {
                                builder.WritePropertyName("Volume");
                                {
                                    builder.WriteValue(44);
                                }
                                builder.WritePropertyName("Height");
                                {
                                    builder.WriteValue(55);
                                }
                                builder.WriteObjectEnd();
                            }
                        }
                        builder.WritePropertyName("@MetaData");
                        {
                            builder.StartWriteObject();
                            {
                                builder.WritePropertyName("Ticks");
                                {
                                    builder.WriteValue(22);
                                }
                                builder.WritePropertyName("Tacks");
                                {
                                    builder.WriteValue(11);
                                }
                                builder.WriteObjectEnd();
                            }
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();
                    var reader = builder.CreateReader();
                    var stream = new MemoryStream();


                    Assert.Equal(2, reader.Count);

                    var data = reader["Data"] as BlittableJsonReaderObject;
                    Assert.Equal(44, int.Parse(data["Volume"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(55, int.Parse(data["Height"].ToString(), CultureInfo.InvariantCulture));

                    var metadata = reader["@MetaData"] as BlittableJsonReaderObject;
                    Assert.Equal(22, int.Parse(metadata["Ticks"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(11, int.Parse(metadata["Tacks"].ToString(), CultureInfo.InvariantCulture));
                }
            }
        }

        [Fact]
        public void BasicIntDeeperNestedStructure()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MegaData");
                        {
                            builder.StartWriteObject();
                            {
                                builder.WritePropertyName("Data");
                                {
                                    builder.StartWriteObject();
                                    {
                                        builder.WritePropertyName("Volume");
                                        {
                                            builder.WriteValue(44);
                                        }
                                        builder.WritePropertyName("Height");
                                        {
                                            builder.WriteValue(55);
                                        }
                                    }
                                }
                                builder.WriteObjectEnd();
                                builder.WritePropertyName("@MetaData");
                                {
                                    builder.StartWriteObject();
                                    {
                                        builder.WritePropertyName("Ticks");
                                        {
                                            builder.WriteValue(22);
                                        }
                                        builder.WritePropertyName("Tacks");
                                        {
                                            builder.WriteValue(11);
                                        }
                                        builder.WriteObjectEnd();
                                    }
                                }
                            }
                            builder.WriteObjectEnd();
                        }
                        builder.WritePropertyName("MegaMetaData");
                        {
                            builder.StartWriteObject();
                            {
                                builder.WritePropertyName("MetaObject");
                                {
                                    builder.StartWriteObject();
                                    {
                                        builder.WritePropertyName("Age");
                                        {
                                            builder.WriteValue(78);
                                        }
                                        builder.WritePropertyName("Code");
                                        {
                                            builder.WriteValue(100);
                                        }
                                        builder.WriteObjectEnd();
                                    }
                                    builder.WritePropertyName("@MetaMetaData");
                                    {
                                        builder.StartWriteObject();
                                        {
                                            builder.WritePropertyName("Tricks");
                                            {
                                                builder.WriteValue(2);
                                            }
                                            builder.WritePropertyName("Tracks");
                                            {
                                                builder.WriteValue(111);
                                            }
                                            builder.WriteObjectEnd();
                                        }
                                    }
                                }
                                builder.WriteObjectEnd();
                            }
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();
                    using (var reader = builder.CreateReader())
                    {
                        Assert.Equal(2, reader.Count);

                        var megaData = reader["MegaData"] as BlittableJsonReaderObject;

                        var data = megaData["Data"] as BlittableJsonReaderObject;
                        Assert.Equal(2, data.Count);
                        Assert.Equal(44, int.Parse(data["Volume"].ToString(), CultureInfo.InvariantCulture));
                        Assert.Equal(55, int.Parse(data["Height"].ToString(), CultureInfo.InvariantCulture));

                        var metadata = megaData["@MetaData"] as BlittableJsonReaderObject;
                        Assert.Equal(2, metadata.Count);
                        Assert.Equal(22, int.Parse(metadata["Ticks"].ToString(), CultureInfo.InvariantCulture));
                        Assert.Equal(11, int.Parse(metadata["Tacks"].ToString(), CultureInfo.InvariantCulture));

                        var megaMetaData = reader["MegaMetaData"] as BlittableJsonReaderObject;
                        Assert.Equal(2, megaMetaData.Count);

                        var metaObject = megaMetaData["MetaObject"] as BlittableJsonReaderObject;
                        Assert.Equal(2, metaObject.Count);
                        Assert.Equal(78, int.Parse(metaObject["Age"].ToString(), CultureInfo.InvariantCulture));
                        Assert.Equal(100, int.Parse(metaObject["Code"].ToString(), CultureInfo.InvariantCulture));

                        var metaMetaData = megaMetaData["@MetaMetaData"] as BlittableJsonReaderObject;
                        Assert.Equal(2, metaMetaData.Count);
                        Assert.Equal(2, int.Parse(metaMetaData["Tricks"].ToString(), CultureInfo.InvariantCulture));
                        Assert.Equal(111, int.Parse(metaMetaData["Tracks"].ToString(), CultureInfo.InvariantCulture));
                    }
                }
            }
        }

        [Fact]
        public void FlatObjectWithEmptyArray()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyEmptyArray");
                        {
                            builder.StartWriteArray();
                            {
                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(2, reader.Count);

                    var array = reader["MyEmptyArray"] as BlittableJsonReaderArray;
                    Assert.Equal(0, array.Length);

                    Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));

                }
            }
        }

        [Fact]
        public void FlatObjectWithArrayOfEmptyObjects()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyArray");
                        {
                            builder.StartWriteArray();
                            {
                                for (var i = 0; i < 8; i++)
                                {
                                    builder.StartWriteObject();
                                    builder.WriteObjectEnd();
                                }
                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    using (var reader = builder.CreateReader())
                    {

                        Assert.Equal(2, reader.Count);

                        var array = reader["MyArray"] as BlittableJsonReaderArray;
                        Assert.Equal(8, array.Length);

                        for (var i = 0; i < 8; i++)
                        {
                            var nested = array[i] as BlittableJsonReaderObject;
                            Assert.Equal(0, nested.Count);
                        }

                        Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));
                    }

                }
            }
        }

        [Fact]
        public void FlatObjectWithIntArrayTest()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyNumbers");
                        {
                            builder.StartWriteArray();
                            {
                                for (var i = 0; i < 8; i++)
                                    builder.WriteValue(i);

                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(2, reader.Count);

                    var array = reader["MyNumbers"] as BlittableJsonReaderArray;
                    Assert.Equal(8, array.Length);
                    for (var i = 0; i < 8; i++)
                        Assert.Equal(i, int.Parse(array[i].ToString(), CultureInfo.InvariantCulture));

                    Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));

                }
            }
        }


        [Fact]
        public void ObjectWithNestedIntArrayTest()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyNestedArrayOfNumbers");
                        {
                            builder.StartWriteArray();
                            {
                                for (var i = 0; i < 8; i++)
                                {
                                    builder.StartWriteArray();
                                    {
                                        for (var j = 0; j < 8; j++)
                                            builder.WriteValue(j);

                                        builder.WriteArrayEnd();
                                    }
                                }

                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(2, reader.Count);

                    var array = reader["MyNestedArrayOfNumbers"] as BlittableJsonReaderArray;
                    Assert.Equal(8, array.Length);

                    for (var i = 0; i < 8; i++)
                    {
                        var innerArray = array[i] as BlittableJsonReaderArray;

                        for (var j = 0; j < 8; j++)
                            Assert.Equal(i, int.Parse(innerArray[i].ToString(), CultureInfo.InvariantCulture));

                    }
                    Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));
                }
            }
        }

        [Fact]
        public void FlatObjectWithObjectArray()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyObjects");
                        {
                            builder.StartWriteArray();
                            {
                                for (var i = 0; i < 8; i++)
                                {
                                    builder.StartWriteObject();
                                    {
                                        builder.WritePropertyName("NestedNode");
                                        {
                                            builder.WriteValue(i);
                                        }
                                        builder.WriteObjectEnd();
                                    }
                                }

                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    using (var reader = builder.CreateReader())
                    {
                        Assert.Equal(2, reader.Count);

                        var array = reader["MyObjects"] as BlittableJsonReaderArray;
                        Assert.Equal(8, array.Length);
                        for (var i = 0; i < 8; i++)
                        {
                            var nested = array[i] as BlittableJsonReaderObject;
                            Assert.Equal(i, int.Parse(nested["NestedNode"].ToString(), CultureInfo.InvariantCulture));
                        }


                        Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));
                    }
                }
            }
        }

        [Fact]
        public void FlatObjectWithObjectArrayWithNestedArray()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();

                    builder.StartWriteObject();
                    {
                        builder.WritePropertyName("MyObjects");
                        {
                            builder.StartWriteArray();
                            {
                                for (var i = 0; i < 8; i++)
                                {
                                    builder.StartWriteObject();
                                    {
                                        builder.WritePropertyName("NestedNode");
                                        {
                                            builder.StartWriteArray();
                                            {
                                                for (var j = 0; j < 8; j++)
                                                {
                                                    builder.WriteValue(j);
                                                }
                                                builder.WriteArrayEnd();
                                            }

                                        }
                                        builder.WriteObjectEnd();
                                    }
                                }

                                builder.WriteArrayEnd();
                            }
                        }
                        builder.WritePropertyName("Height");
                        {
                            builder.WriteValue(55);
                        }
                        builder.WriteObjectEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();

                    Assert.Equal(2, reader.Count);

                    var array = reader["MyObjects"] as BlittableJsonReaderArray;
                    Assert.Equal(8, array.Length);
                    for (var i = 0; i < 8; i++)
                    {
                        var nested = array[i] as BlittableJsonReaderObject;
                        var nestedArray = nested["NestedNode"] as BlittableJsonReaderArray;
                        for (int j = 0; j < 8; j++)
                        {
                            Assert.Equal(j, int.Parse(nestedArray[j].ToString(), CultureInfo.InvariantCulture));
                        }
                    }

                    Assert.Equal(55, int.Parse(reader["Height"].ToString(), CultureInfo.InvariantCulture));
                }
            }
        }

        [Fact]
        public void SimpleArrayDocument()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartArrayDocument();

                    builder.StartWriteArray();
                    {
                        for (var i = 0; i < 8; i++)
                            builder.WriteValue(i);
                        builder.WriteArrayEnd();
                    }
                    builder.FinalizeDocument();

                    var reader = builder.CreateArrayReader();

                    Assert.Equal(8, reader.Length);

                    for (var i = 0; i < 8; i++)
                        Assert.Equal(i, int.Parse(reader[i].ToString(), CultureInfo.InvariantCulture));

                }
            }
        }


        [Theory]
        [InlineData(byte.MaxValue)]
        [InlineData(short.MaxValue)]
        [InlineData(short.MaxValue + 1)]
        public void BigDepthTest(int propertiesAmount)
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();
                    
                    for (int i = 0; i < propertiesAmount; i++)
                    {
                        builder.WritePropertyName("Data" + i);
                        builder.StartWriteObject();
                        builder.WritePropertyName("Age" + i);
                        builder.WriteValue(i);
                    }

                    for (int i = 0; i < propertiesAmount; i++)
                    {
                        builder.WriteObjectEnd();
                    }

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();
                    Assert.Equal(1, reader.Count);
                    for (var i = 0; i < propertiesAmount; i++)
                    {
                        reader = reader["Data" + i] as BlittableJsonReaderObject;
                        var val = reader["Age" + i];
                        Assert.Equal(i, int.Parse(val.ToString(), CultureInfo.InvariantCulture));
                    }

                }
            }
        }

        [Fact]
        public unsafe void ReadDataTypesTest()
        {
            using (var context = new JsonOperationContext(1024, 1024 * 4, SharedMultipleUseFlag.None))
            {
                BlittableJsonReaderObject embeddedReader;
                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();
                    builder.WritePropertyName("Value");
                    builder.WriteValue(1000);
                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();
                    embeddedReader = builder.CreateReader();
                }

                using (var builder = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(context))
                {
                    var lonEscapedCharsString = string.Join(",", Enumerable.Repeat("\"Cool\"", 200).ToArray());
                    var longEscapedCharsAndNonAsciiString = string.Join(",", Enumerable.Repeat("\"מגניב\"", 200).ToArray());

                    builder.Reset(BlittableJsonDocumentBuilder.UsageMode.None);

                    builder.StartWriteObjectDocument();
                    builder.StartWriteObject();

                    builder.WritePropertyName("FloatMin");
                    builder.WriteValue(float.MinValue);

                    builder.WritePropertyName("FloatMax");
                    builder.WriteValue(float.MaxValue);

                    builder.WritePropertyName("UshortMin");
                    builder.WriteValue(ushort.MinValue);

                    builder.WritePropertyName("UshortMax");
                    builder.WriteValue(ushort.MaxValue);

                    builder.WritePropertyName("UintMin");
                    builder.WriteValue(uint.MinValue);

                    builder.WritePropertyName("UintMax");
                    builder.WriteValue(uint.MaxValue);

                    builder.WritePropertyName("DoubleMin");
                    builder.WriteValue(double.MinValue);

                    builder.WritePropertyName("DoubleMax");
                    builder.WriteValue(double.MaxValue);

                    builder.WritePropertyName("LongMin");
                    builder.WriteValue(long.MinValue);

                    builder.WritePropertyName("LongMax");
                    builder.WriteValue(long.MaxValue);

                    builder.WritePropertyName("StringEmpty");
                    builder.WriteValue(string.Empty);

                    builder.WritePropertyName("StringSimple");
                    builder.WriteValue("StringSimple");

                    builder.WritePropertyName("StringEscapedChars");
                    builder.WriteValue("\"Cool\"");

                    builder.WritePropertyName("StringLongEscapedChars");
                    builder.WriteValue(lonEscapedCharsString);

                    builder.WritePropertyName("StringEscapedCharsAndNonAscii");
                    builder.WriteValue(longEscapedCharsAndNonAsciiString);


                    var lsvString = "\"fooאbar\"";
                    var lsvStringBytes = Encoding.UTF8.GetBytes(lsvString);
                    fixed (byte* b = lsvStringBytes)
                    {
                        var escapePositionsMaxSize = JsonParserState.FindEscapePositionsMaxSize(lsvString);
                        var lsv = context.AllocateStringValue(null,b,lsvStringBytes.Length);
                        var escapePositions = new FastList<int>();
                        var len = lsvStringBytes.Length;
                        JsonParserState.FindEscapePositionsIn(escapePositions, b, ref len, escapePositionsMaxSize);
                        lsv.EscapePositions = escapePositions.ToArray();

                        builder.WritePropertyName("LSVString");
                        builder.WriteValue(lsv);
                    }

                    builder.WritePropertyName("Embedded");
                    builder.WriteEmbeddedBlittableDocument(embeddedReader);

                    builder.WriteObjectEnd();
                    builder.FinalizeDocument();

                    var reader = builder.CreateReader();
                    reader.BlittableValidation();

                    Assert.Equal(17, reader.Count);
                    Assert.Equal(float.MinValue, float.Parse(reader["FloatMin"].ToString(),CultureInfo.InvariantCulture));
                    Assert.Equal(float.MaxValue, float.Parse(reader["FloatMax"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(ushort.MinValue, ushort.Parse(reader["UshortMin"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(ushort.MaxValue, ushort.Parse(reader["UshortMax"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(uint.MinValue, uint.Parse(reader["UintMin"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(uint.MaxValue, uint.Parse(reader["UintMax"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(double.MinValue, double.Parse(reader["DoubleMin"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(double.MaxValue, double.Parse(reader["DoubleMax"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(long.MinValue, long.Parse(reader["LongMin"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(long.MaxValue, long.Parse(reader["LongMax"].ToString(), CultureInfo.InvariantCulture));
                    Assert.Equal(string.Empty, reader["StringEmpty"].ToString());
                    Assert.Equal("StringSimple", reader["StringSimple"].ToString());
                    Assert.Equal("\"Cool\"", reader["StringEscapedChars"].ToString());
                    Assert.Equal(lonEscapedCharsString, reader["StringLongEscapedChars"].ToString());
                    Assert.Equal(longEscapedCharsAndNonAsciiString, reader["StringEscapedCharsAndNonAscii"].ToString());
                    Assert.Equal(lsvString, reader["LSVString"].ToString());
                    Assert.Equal(1000, int.Parse((reader["Embedded"] as BlittableJsonReaderObject)["Value"].ToString(), CultureInfo.InvariantCulture));
                }
            }
        }
    }
}
