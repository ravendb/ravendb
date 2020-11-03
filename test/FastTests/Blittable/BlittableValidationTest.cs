using System;
using System.IO;
using System.Linq;
using System.Text;
using FastTests.Blittable.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class BlittableValidationTest : NoDisposalNeeded
    {
        public BlittableValidationTest(ITestOutputHelper output) : base(output)
        {
        }

        private BlittableJsonReaderObject InitSimpleBlittable(JsonOperationContext context, out int size)
        {
            var obj = JObject.FromObject(new Employee
            {
                Id = "1",
                FirstName = "Hibernating",
                LastName = "Rhinos"
            });
            var objString = obj.ToString(Formatting.None);
            var stream = new MemoryStream();
            var streamWriter = new StreamWriter(stream);
            streamWriter.Write(objString);
            streamWriter.Flush();
            stream.Position = 0;
            var reader = context.Sync.ReadForDisk(stream, "docs/1 ");
            size = reader.Size;
            return reader;
        }

        [Fact]
        public void WithEscape()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var obj = JObject.FromObject(new Employee
                {
                    Id = "1",
                    FirstName = "Hibernating\nRhinos",
                    LastName = "Rhinos"
                });
                var objString = obj.ToString(Formatting.None);
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(objString);
                streamWriter.Flush();
                stream.Position = 0;
                var reader = context.Sync.ReadForDisk(stream, "docs/1 ");
                reader.BlittableValidation();
            }
        }

        private class AllTokensTypes
        {
            public bool Bool { get; set; }
            public string Null { get; set; }
            public string String { get; set; }
            public int Int { get; set; }
            public float Float { get; set; }
            public int[] IntArray { get; set; }
            public Empty Object { get; set; }
        }

        private class Str
        {
            public string str { get; set; }
        }

        private static Company InitCompany()
        {
            var city = new City
            {
                CountryId = "3123456789",
                Id = "123654789",
                Name = "Haifa",
                RegionId = "2"
            };
            var region = new Region { CountryId = "3123456789", Id = "24682468", Name = "North" };
            var address = new AddressClass
            {
                City = city,
                Country = "3123456789",
                Region = region,
                Line1 = "Ilnot",
                Line2 = "21",
                PostalCode = 12345
            };
            var contact = new ContactClass
            {
                Name = "abcdefghi",
                Title = "jklmnopqrst"
            };

            var employees = new Employee[10];
            for (var i = 0; i < 10; i++)
            {
                employees[i] = new Employee
                {
                    Id = "aaaa",
                    FirstName = "bbbb",
                    LastName = "cccc"
                };
            }

            var company = new Company
            {
                Address = address,
                Name = "Hibernating Rhinos",
                Contact = contact,
                EmployeesIds = employees,
                Phone = "054111222",
                ExternalId = "123123123",
                Fax = "456456456",
                Type = Company.CompanyType.Public
            };
            return company;
        }

        private class Empty
        {
        }

        private class City
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string RegionId { get; set; }
            public string CountryId { get; set; }
        }

        private class Region
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CountryId { get; set; }
        }

        private class ContactClass
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }

        private class AddressClass
        {
            public string Line1 { get; set; }
            public string Line2 { get; set; }
            public City City { get; set; }
            public Region Region { get; set; }
            public int PostalCode { get; set; }
            public string Country { get; set; }
        }

        private class Company
        {
            public enum CompanyType
            {
                Public,
                Private
            }

            public string ExternalId { get; set; }
            public string Name { get; set; }
            public ContactClass Contact { get; set; }
            public AddressClass Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
            public Company.CompanyType Type { get; set; }
            public Employee[] EmployeesIds { get; set; }
        }

        private class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        [Fact]
        public void Complex_Valid_Blittable()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var company = InitCompany();
                var obj = JObject.FromObject(company);
                var objString = obj.ToString(Formatting.None);
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(objString);
                streamWriter.Flush();
                stream.Position = 0;
                var reader = context.Sync.ReadForDisk(stream, "docs/1 ");
                reader.BlittableValidation();
            }
        }

        [Fact]
        public void Empty_Valid_Blittable()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var obj = JObject.FromObject(new Empty());
                var objString = obj.ToString(Formatting.None);
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(objString);
                streamWriter.Flush();
                stream.Position = 0;

                var reader = context.Sync.ReadForDisk(stream, "docs/1 ");
                reader.BlittableValidation();
            }
        }

        [Fact]
        public unsafe void Invalid_Bool()
        {
            var blittable = new byte[16]
            {
                0x02, 0x01, 0x01, 0x00, 0x07, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x01, 0x0b, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x10, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Bool not valid");
            }
        }

        [Fact]
        public unsafe void Invalid_Compressed_String()
        {
            var blittable = new byte[31]
            {
                0x04, 0x0d, 0x1f, 0x61, 0x01, 0x00, 0xff, 0xff, 0x5f, 0x50,
                0x61, 0x61, 0x61, 0x61, 0x61, 0x00, 0x01, 0x10, 0x00, 0x06,
                0x04, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x10, 0x06, 0x10, 0x1A,
                0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1F, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Compressed string not valid");
            }
        }

        [Fact]
        public unsafe void Invalid_Float()
        {
            var blittable = new byte[22]
            {
                0x05, 0x31, 0x2e, 0x33, 0x2e, 0x35, 0x00, 0x01, 0x07, 0x00,
                0x04, 0x04, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x10, 0x06, 0x07,
                0x11, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var reader = new BlittableJsonReaderObject(ptr, 0x16, context);
                    var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                    Assert.Equal(message.Message, "Number not valid (1.3.5)");
                }
            }
        }

        [Fact]
        public unsafe void Invalid_Integer()
        {
            var blittable = new byte[26]
            {
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xd0, 0x86, 0x03,
                0x01, 0x0b, 0x00, 0x03, 0x04, 0x74, 0x65,
                0x6d, 0x70, 0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1A, null);
                var message = Assert.Throws<FormatException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Bad variable size int");
            }
        }

        [Fact]
        public unsafe void Invalid_Long()
        {
            var blittable = new byte[26]
            {
                0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x01, 0x01, 0x0b, 0x00, 0x03, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1a, null);
                var message = Assert.Throws<FormatException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Bad variable size int");
            }
        }

        [Fact]
        public unsafe void Invalid_Names_Offset()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                int size;
                var reader = InitSimpleBlittable(context, out size);
                var basePointer = reader.BasePointer;

                *(basePointer + size - 7) = 0x11;
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Properties names token not valid");

                *(basePointer + size - 7) = 0x50;
                message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Properties names token not valid");

                *(basePointer + size - 7) = 0x00;
                var messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
                Assert.Equal(messageArg.Message, "Illegal offset size 0");

                *(basePointer + size - 7) = 0x10;
                var listStart = size - 6;

                for (var i = 0; i < 2; i++)
                {
                    var temp = basePointer[listStart + i];

                    basePointer[listStart + i] = (byte)listStart;
                    message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                    Assert.Equal(message.Message, "Properties names offset not valid");

                    basePointer[listStart + i] = temp;
                }

                for (var i = 0; i < 1; i++)
                {
                    var temp = basePointer[listStart + i];

                    basePointer[listStart + i] = basePointer[listStart + i + 1];
                    message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                    Assert.Equal(message.Message, "Properties names offset not valid");

                    basePointer[listStart + i] = temp;
                }
            }
        }

        [Fact]
        public unsafe void Invalid_Null()
        {
            var blittable = new byte[16]
            {
                0x01, 0x01, 0x01, 0x00, 0x08, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x01, 0x0b, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x10, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Null not valid");
            }
        }

        [Fact]
        public unsafe void Invalid_Number_Of_Prop()
        {
            var blittable = new byte[192]
            {
                0x0c, 0x46, 0x69, 0x72, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65,
                0x20, 0x23, 0x30, 0x00, 0x0b, 0x4c, 0x61, 0x73, 0x74, 0x4e,
                0x61, 0x6d, 0x65, 0x20, 0x23, 0x30, 0x00, 0x00, 0x05, 0x55,
                0x73, 0x65, 0x72, 0x73, 0x00, 0x1d, 0x54, 0x72, 0x79, 0x6f,
                0x75, 0x74, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x67, 0x72, 0x61,
                0x6d, 0x2b, 0x55, 0x73, 0x65, 0x72, 0x2c, 0x20, 0x54, 0x72,
                0x79, 0x6f, 0x75, 0x74, 0x73, 0x00, 0x07, 0x75, 0x73, 0x65,
                0x72, 0x73, 0x2f, 0x30, 0x00, 0x03, 0x09, 0x06, 0x05, 0x28,
                0x05, 0x05, 0x2f, 0x04, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F,
                0x0a, 0x03, 0x51, 0x55, 0x00, 0x05, 0x47, 0x01, 0x05, 0x3a,
                0x02, 0x08, 0x09, 0x46, 0x69, 0x72, 0x73, 0x74, 0x4e, 0x61,
                0x6d, 0x65, 0x00, 0x08, 0x4c, 0x61, 0x73, 0x74, 0x4e, 0x61,
                0x6d, 0x65, 0x00, 0x04, 0x54, 0x61, 0x67, 0x73, 0x00, 0x09,
                0x40, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x00,
                0x11, 0x52, 0x61, 0x76, 0x65, 0x6e, 0x2d, 0x45, 0x6e, 0x74,
                0x69, 0x74, 0x79, 0x2d, 0x4e, 0x61, 0x6d, 0x65, 0x00, 0x0e,
                0x52, 0x61, 0x76, 0x65, 0x6e, 0x2d, 0x43, 0x6c, 0x72, 0x2d,
                0x54, 0x79, 0x70, 0x65, 0x00, 0x03, 0x40, 0x69, 0x64, 0x00,
                0x10, 0x4e, 0x43, 0x39, 0x33, 0x28, 0x15, 0x05, 0x55, 0x01,
                0xb4, 0x51
            };
            var size = 0xc0;

            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, size, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Number of properties not valid");
            }
        }

        [Fact]
        public unsafe void Invalid_Root_Token()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                int size;
                var reader = InitSimpleBlittable(context, out size);
                var basePointer = reader.BasePointer;

                *(basePointer + size - 1) = 0x52;
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Illegal root object");

                *(basePointer + size - 1) = 0x41;
                var messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
                Assert.Equal(messageArg.Message, "Illegal offset size StartObject, PropertyIdSizeByte");

                *(basePointer + size - 1) = 0x11;
                messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
                Assert.Equal(messageArg.Message, "Illegal offset size StartObject, OffsetSizeByte");
            }
        }

        [Fact]
        public unsafe void Invalid_String()
        {
            var blittable = new byte[26]
            {
                0x08, 0x61, 0x62, 0x63, 0x64, 0x0a, 0x61, 0x62, 0x63, 0x01,
                0x03, 0x01, 0x0b, 0x00, 0x05, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1A, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.StartsWith("String not valid", message.Message);
            }
        }

        [Fact]
        public void Simple_Valid_Blittable_Test()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                int size;
                var reader = InitSimpleBlittable(context, out size);
                reader.BlittableValidation();
            }
        }

        [Fact]
        public void Valid_Blittable()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var allTokens = new AllTokensTypes
                {
                    Bool = true,
                    Float = 123.4567F,
                    Int = 45679123,
                    IntArray = new[] { 1, 2, 3 },
                    Null = null,
                    Object = new Empty(),
                    String = "qwertyuio"
                };
                var obj = JObject.FromObject(allTokens);
                var objString = obj.ToString(Formatting.None);
                var stream = new MemoryStream();
                var streamWriter = new StreamWriter(stream);
                streamWriter.Write(objString);
                streamWriter.Flush();
                stream.Position = 0;
                var reader = context.Sync.ReadForDisk(stream, "docs/1 ");
                reader.BlittableValidation();
            }
        }

        [Fact]
        public unsafe void Valid_Compressed_String()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    var temp = new Str
                    {
                        str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    };
                    var obj = JObject.FromObject(temp);
                    var objString = obj.ToString(Formatting.None);
                    var buffer = Encoding.UTF8.GetBytes(objString);
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);
                        using (var writer = new BlittableJsonDocumentBuilder(ctx,
                            BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings,
                            "test", parser, state))
                        {
                            writer.ReadObjectDocument();
                            var x = writer.Read();
                            writer.FinalizeDocument();
                            using (var reader = writer.CreateReader())
                                reader.BlittableValidation();
                        }
                    }
                }
            }
        }

        [Fact]
        public unsafe void Valid_object_read_from_non_zero_offset()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var jsonParserState = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(context, jsonParserState, "changes/1"))
                {
                    byte[] buffer = new byte[4096];
                    var bufferOffset = 128; //non-zero offset

                    var allTokens = new AllTokensTypes
                    {
                        Bool = true,
                        Float = 123.4567F,
                        Int = 45679123,
                        IntArray = new[] { 1, 2, 3 },
                        Null = null,
                        Object = new Empty(),
                        String = "qwertyuio"
                    };
                    var obj = JObject.FromObject(allTokens);
                    var objString = obj.ToString(Formatting.None);

                    var data = Encoding.UTF8.GetBytes(objString);

                    data.CopyTo(buffer, bufferOffset);

                    fixed (byte* pBuffer = buffer)
                    {
                        using (var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None, "order/1", parser, jsonParserState))
                        {
                            parser.SetBuffer(pBuffer + bufferOffset, data.Length);

                            builder.ReadObjectDocument();

                            Assert.True(builder.Read());

                            builder.FinalizeDocument();

                            using (var reader = builder.CreateReader())
                            {
                                var value = reader.ToString();
                                Assert.NotNull(value);
                            }
                        }
                    }
                }
            }
        }

        [Fact]
        public unsafe void Valid_String()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    var temp = new Str
                    {
                        str = "\nabcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz" +
                              "abcdefghijklmnopqrstuvwxyz\n"
                    };
                    var obj = JObject.FromObject(temp);
                    var objString = obj.ToString(Formatting.None);
                    var buffer = Encoding.UTF8.GetBytes(objString);
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);
                        using (var writer = new BlittableJsonDocumentBuilder(ctx,
                            BlittableJsonDocumentBuilder.UsageMode.None,
                            "test", parser, state))
                        {
                            writer.ReadObjectDocument();
                            var x = writer.Read();
                            writer.FinalizeDocument();
                            using (var reader = writer.CreateReader())
                                reader.BlittableValidation();
                        }
                    }
                }
            }
        }

        private string createRandomEscString(int length)
        {
            Random random = new Random();
            char[] EscapeChars = { '\b', '\t', '\r', '\n', '\f', '\\', '"', };
            return new string(Enumerable.Repeat(EscapeChars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        [Fact]
        public unsafe void Valid_String_With_260_EscChar()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    var temp = new Str
                    {
                        str = createRandomEscString(260)
                    };
                    var obj = JObject.FromObject(temp);
                    var objString = obj.ToString(Formatting.None);
                    var buffer = Encoding.UTF8.GetBytes(objString);
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);
                        using (var writer = new BlittableJsonDocumentBuilder(ctx,
                            BlittableJsonDocumentBuilder.UsageMode.None,
                            "test", parser, state))
                        {
                            writer.ReadObjectDocument();
                            writer.Read();
                            writer.FinalizeDocument();
                            using (var reader = writer.CreateReader())
                                reader.BlittableValidation();
                        }
                    }
                }
            }
        }

        [Fact]
        public unsafe void Valid_String_With_66K_EscChar()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    var temp = new Str
                    {
                        str = createRandomEscString(66 * 1024)
                    };

                    var obj = JObject.FromObject(temp);
                    var objString = obj.ToString(Formatting.None);
                    var buffer = Encoding.UTF8.GetBytes(objString);
                    fixed (byte* pBuffer = buffer)
                    {
                        parser.SetBuffer(pBuffer, buffer.Length);
                        using (var writer = new BlittableJsonDocumentBuilder(ctx,
                            BlittableJsonDocumentBuilder.UsageMode.None,
                            "test", parser, state))
                        {
                            writer.ReadObjectDocument();
                            var x = writer.Read();
                            writer.FinalizeDocument();
                            using (var reader = writer.CreateReader())
                                reader.BlittableValidation();
                        }
                    }
                }
            }
        }

        [Fact]
        public unsafe void Invalid_Props_Offset()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                int size;
                var reader = InitSimpleBlittable(context, out size);
                var basePointer = reader.BasePointer;

                *(basePointer + size - 2) = 0xbc;
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Properties names offset not valid");

                *(basePointer + size - 2) = 0x00;
                message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Properties names offset not valid");
            }
        }

        [Fact]
        public unsafe void Invalid_Root_Metadata_Offset()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                int size;
                var reader = InitSimpleBlittable(context, out size);
                var basePointer = reader.BasePointer;

                *(basePointer + size - 3) = 0x40;
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Root metadata offset not valid");
            }
        }

        [Fact]
        public unsafe void ParseBlitAndValidate()
        {
            var assembly = typeof(BlittableFormatTests).Assembly;

            var resources = assembly.GetManifestResourceNames();
            var resourcePrefix = typeof(BlittableFormatTests).Namespace + ".Jsons.";

            foreach (var name in resources.Where(x => x.StartsWith(resourcePrefix, StringComparison.Ordinal)))
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    using (var stream = assembly.GetManifestResourceStream(name))
                    {
                        using (var obj = context.Sync.ReadForDisk(stream, "docs/1"))
                        {
                            obj.BlittableValidation();
                        }
                    }
                }
            }
        }
    }
}
