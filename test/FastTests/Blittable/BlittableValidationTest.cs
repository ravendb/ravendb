using System;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Sparrow.Json;
using Xunit;
using Raven.Json.Linq;
using Sparrow.Compression;
using Sparrow.Json.Parsing;

namespace FastTests.Blittable
{
    public class BlittableValidationTest : RavenTestBase
    {
        public int Size = 0xbc;
        public byte[] SimpleValidBlittable = new byte[190]
        {
            0x0c, 0x46, 0x69, 0x72, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65,
            0x20, 0x23, 0x30, 0x00, 0x0b, 0x4c, 0x61, 0x73, 0x74, 0x4e,
            0x61, 0x6d, 0x65, 0x20, 0x23, 0x30, 0x00, 0x00, 0x05, 0x55,
            0x73, 0x65, 0x72, 0x73, 0x00, 0x1d, 0x54, 0x72, 0x79, 0x6f,
            0x75, 0x74, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x67, 0x72, 0x61,
            0x6d, 0x2b, 0x55, 0x73, 0x65, 0x72, 0x2c, 0x20, 0x54, 0x72,
            0x79, 0x6f, 0x75, 0x74, 0x73, 0x00, 0x07, 0x75, 0x73, 0x65,
            0x72, 0x73, 0x2f, 0x30, 0x00, 0x03, 0x09, 0x06, 0x05, 0x28,
            0x05, 0x05, 0x2f, 0x04, 0x05, 0x04, 0x0a, 0x03, 0x51, 0x55,
            0x00, 0x05, 0x47, 0x01, 0x05, 0x3a, 0x02, 0x08, 0x09, 0x46,
            0x69, 0x72, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x00, 0x08,
            0x4c, 0x61, 0x73, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x00, 0x04,
            0x54, 0x61, 0x67, 0x73, 0x00, 0x09, 0x40, 0x6d, 0x65, 0x74,
            0x61, 0x64, 0x61, 0x74, 0x61, 0x00, 0x11, 0x52, 0x61, 0x76,
            0x65, 0x6e, 0x2d, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x2d,
            0x4e, 0x61, 0x6d, 0x65, 0x00, 0x0e, 0x52, 0x61, 0x76, 0x65,
            0x6e, 0x2d, 0x43, 0x6c, 0x72, 0x2d, 0x54, 0x79, 0x70, 0x65,
            0x00, 0x03, 0x40, 0x69, 0x64, 0x00, 0x10, 0x4e, 0x43, 0x39,
            0x33, 0x28, 0x15, 0x05, 0x55, 0x01, 0xb0, 0x51, 0x00, 0x00
        };

        [Fact]
        public unsafe void EmptyValiedBlittable()
        {
            try
            {
                using (var pool = new UnmanagedBuffersPool("test"))
                using (var context = new JsonOperationContext(pool))
                {
                    var obj = RavenJObject.FromObject(new Empty());
                    var objString = obj.ToString(Formatting.None);
                    var stream = new MemoryStream();
                    var streamWriter = new StreamWriter(stream);
                    streamWriter.Write(objString);
                    streamWriter.Flush();
                    stream.Position = 0;
                    var reader = context.Read(stream, "docs/1 ");
                    reader.BlittableValidation(reader.Size);
                }
            }
            catch (Exception)
            {
                Assert.False(true);
            }
        }

        [Fact]
        public unsafe void SimpleValidBlittableTest()
        {
            try
            {
                fixed (byte* ptr = &SimpleValidBlittable[0])
                {
                    var reader = new BlittableJsonReaderObject(ptr, Size, null);
                    reader.BlittableValidation(Size);
                }
            }
            catch (Exception)
            {
                Assert.False(true);
            }
        }

        [Fact]
        public unsafe void ComplexValidBlittable()
        {
            try
            {
                using (var pool = new UnmanagedBuffersPool("test"))
                using (var context = new JsonOperationContext(pool))
                {
                    var company = InitCompany();
                    var obj = RavenJObject.FromObject(company);
                    var objString = obj.ToString(Formatting.None);
                    var stream = new MemoryStream();
                    var streamWriter = new StreamWriter(stream);
                    streamWriter.Write(objString);
                    streamWriter.Flush();
                    stream.Position = 0;
                    var reader = context.Read(stream, "docs/1 ");
                    reader.BlittableValidation(reader.Size);
                }
            }
            catch (Exception)
            {
                Assert.False(true);
            }
        }

        public class AllTokensTypes
        {
            public bool Bool { get; set; }
            public string Null { get; set; }
            public string String { get; set; }
            public int Int { get; set; }
            public float Float { get; set; }
            public int[] IntArray { get; set; }
            public Empty Object { get; set; }
        }

        [Fact]
        public unsafe void ValidBlittable()
        {
            try
            {
                using (var pool = new UnmanagedBuffersPool("test"))
                using (var context = new JsonOperationContext(pool))
                {
                    var allTokens = new AllTokensTypes()
                    {
                        Bool = true,
                        Float = 123.4567F,
                        Int = 45679123,
                        IntArray = new[] { 1, 2, 3 },
                        Null = null,
                        Object = new Empty(),
                        String = "qwertyuio"
                    };
                    var obj = RavenJObject.FromObject(allTokens);
                    var objString = obj.ToString(Formatting.None);
                    var stream = new MemoryStream();
                    var streamWriter = new StreamWriter(stream);
                    streamWriter.Write(objString);
                    streamWriter.Flush();
                    stream.Position = 0;
                    var reader = context.Read(stream, "docs/1 ");
                    reader.BlittableValidation(reader.Size);
                }
            }
            catch (Exception)
            {
                Assert.False(true);
            }
        }

        [Fact]
        public unsafe void InvalidRootToken()
        {
            fixed (byte* ptr = &SimpleValidBlittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, Size, null);
                Action<int> test = reader.BlittableValidation;

                SimpleValidBlittable[187] = 0x52;
                var message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Illegal root object");

                SimpleValidBlittable[187] = 0x41;
                var messageArg = Assert.Throws<ArgumentException>(() => test(Size));
                Assert.Equal(messageArg.Message, "Illegal offset size");

                SimpleValidBlittable[187] = 0x11;
                messageArg = Assert.Throws<ArgumentException>(() => test(Size));
                Assert.Equal(messageArg.Message, "Illegal offset size");

                SimpleValidBlittable[187] = 0x51;
            }
        }

        [Fact]
        public unsafe void InvalidPropsOffset()
        {
            var temp = SimpleValidBlittable[186];
            fixed (byte* ptr = &SimpleValidBlittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, Size, null);
                Action<int> test = reader.BlittableValidation;

                SimpleValidBlittable[186] = 0xbc;
                var message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Properties names offset not valid");

                SimpleValidBlittable[186] = 0x00;
                message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Properties names offset not valid");

                Size += 1;
                SimpleValidBlittable[185] = 0x10;
                SimpleValidBlittable[186] = 0x80;
                SimpleValidBlittable[187] = 0x80;
                SimpleValidBlittable[188] = 0x51;
                message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Properties names offset not valid");

                Size -= 1;
                SimpleValidBlittable[185] = 0x01;
                SimpleValidBlittable[186] = 0xb0;
                SimpleValidBlittable[187] = 0x51;
                SimpleValidBlittable[188] = 0x00;
            }
        }

        [Fact]
        public unsafe void InvalidRootMetadataOffset()
        {
            fixed (byte* ptr = &SimpleValidBlittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, Size, null);
                Action<int> test = reader.BlittableValidation;

                Size += 1;
                SimpleValidBlittable[184] = 0x01;
                SimpleValidBlittable[185] = 0xbc;
                SimpleValidBlittable[186] = 0x01;
                SimpleValidBlittable[187] = 0xb0;
                SimpleValidBlittable[188] = 0x51;
                var message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Root metadata offset not valid");

                Size += 1;
                SimpleValidBlittable[184] = 0x10;
                SimpleValidBlittable[185] = 0x80;
                SimpleValidBlittable[186] = 0x80;
                SimpleValidBlittable[187] = 0x01;
                SimpleValidBlittable[188] = 0xb0;
                SimpleValidBlittable[189] = 0x51;
                message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Root metadata offset not valid");

                Size -= 2;
                SimpleValidBlittable[184] = 0x55;
                SimpleValidBlittable[185] = 0x01;
                SimpleValidBlittable[186] = 0xb0;
                SimpleValidBlittable[187] = 0x51;
            }
        }

        [Fact]
        public unsafe void InvalidNamesOffset()
        {

            fixed (byte* ptr = &SimpleValidBlittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, Size, null);
                Action<int> test = reader.BlittableValidation;

                SimpleValidBlittable[0xb0] = 0x11;
                var message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Properties names token not valid");

                SimpleValidBlittable[0xb0] = 0x50;
                message = Assert.Throws<InvalidDataException>(() => test(Size));
                Assert.Equal(message.Message, "Properties names token not valid");

                SimpleValidBlittable[0xb0] = 0x00;
                var messageArg = Assert.Throws<ArgumentException>(() => test(Size));
                Assert.Equal(messageArg.Message, "Illegal offset size");

                SimpleValidBlittable[0xb0] = 0x10;
                const byte listStart = 0xb1;

                for (var i = 0; i < 7; i++)
                {
                    var temp = SimpleValidBlittable[listStart + i];

                    SimpleValidBlittable[listStart + i] = (byte)listStart;
                    message = Assert.Throws<InvalidDataException>(() => test(Size));
                    Assert.Equal(message.Message, "Properties names offset not valid");

                    SimpleValidBlittable[listStart + i] = ++temp;
                    message = Assert.Throws<InvalidDataException>(() => test(Size));
                    Assert.Equal(message.Message, "String not valid");

                    SimpleValidBlittable[listStart + i] = --temp;
                }

                for (var i = 0; i < 6; i++)
                {
                    var temp = SimpleValidBlittable[listStart + i];

                    SimpleValidBlittable[listStart + i] = SimpleValidBlittable[listStart + i + 1];
                    message = Assert.Throws<InvalidDataException>(() => test(Size));
                    Assert.Equal(message.Message, "Properties names offset not valid");

                    SimpleValidBlittable[listStart + i] = temp;
                }
            }
        }

        [Fact]
        public unsafe void InvalidNumberOfProp()
        {
            byte[] blittable = new byte[192]
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
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(size));
                Assert.Equal(message.Message, "Number of properties not valid");
            }
        }

        [Fact]
        public unsafe void InvalidInteger()
        {
            byte[] blittable = new byte[26]
            {
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xd0, 0x86, 0x03,
                0x01, 0x0b, 0x00, 0x03, 0x04, 0x74, 0x65,
                0x6d, 0x70, 0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1A, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<FormatException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Bad variable size int");
            }
        }

        [Fact]
        public unsafe void InvalidFloat()
        {
            byte[] blittable = new byte[22]
            {
                0x05, 0x31, 0x2e, 0x33, 0x2e, 0x35, 0x00, 0x01, 0x07, 0x00,
                0x04, 0x04, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x10, 0x06, 0x07,
                0x11, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x16, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Float not valid");
            }
        }

        [Fact]
        public unsafe void InvalidString()
        {
            byte[] blittable = new byte[26]
            {
                0x08, 0x61, 0x62, 0x63, 0x64, 0x0a, 0x61, 0x62, 0x63, 0x01,
                0x03, 0x01, 0x0b, 0x00, 0x05, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1A, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(reader.Size));
                Assert.Equal(message.Message, "String not valid");
            }
        }

        [Fact]
        public unsafe void InvalidBool()
        {
            byte[] blittable = new byte[16]
            {
                0x02, 0x01, 0x01, 0x00, 0x07, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x01, 0x0b, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x10, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Bool not valid");
            }
        }

        [Fact]
        public unsafe void InvalidNull()
        {
            byte[] blittable = new byte[16]
            {
                0x01, 0x01, 0x01, 0x00, 0x08, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x01, 0x0b, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x10, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Null not valid");
            }
        }

        [Fact]
        public unsafe void InvalidCompressedString()
        {
            byte[] blittable = new byte[31]
            {
                0x04, 0x0d, 0x1f, 0x61, 0x01, 0x00, 0xff, 0xff, 0x5f, 0x50,
                0x61, 0x61, 0x61, 0x61, 0x61, 0x00, 0x01, 0x10, 0x00, 0x06,
                0x04, 0x74, 0x65, 0x6d, 0x70, 0x00, 0x10, 0x06, 0x10, 0x1A,
                0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1F, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<InvalidDataException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Compressed string not valid");
            }
        }

        public class Str
        {
            public string temp { get; set; }
        }

        [Fact]
        public unsafe void ValiedCompressedString()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new JsonOperationContext(pool))
            {
                var state = new JsonParserState();
                using (var parser = new UnmanagedJsonParser(ctx, state, "test"))
                {
                    var temp = new Str
                    {
                        temp = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
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
                    var obj = RavenJObject.FromObject(temp);
                    var objString = obj.ToString(Formatting.None);
                    var buffer = Encoding.UTF8.GetBytes(objString);
                    parser.SetBuffer(buffer, buffer.Length);
                    var writer = new BlittableJsonDocumentBuilder(ctx,
                        BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings,
                        "test", parser, state);
                    writer.ReadObject();
                    var x = writer.Read();
                    writer.FinalizeDocument();
                    var reader = writer.CreateReader();
                    try
                    {
                        reader.BlittableValidation(reader.Size);
                    }
                    catch (Exception)
                    {
                        Assert.False(true);
                    }
                }
            }
        }

        [Fact]
        public unsafe void InvalidLong()
        {
            byte[] blittable = new byte[26]
            {
                0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0x01, 0x01, 0x0b, 0x00, 0x03, 0x04, 0x74, 0x65, 0x6d, 0x70,
                0x00, 0x10, 0x06, 0x0b, 0x15, 0x51
            };
            fixed (byte* ptr = &blittable[0])
            {
                var reader = new BlittableJsonReaderObject(ptr, 0x1a, null);
                Action<int> test = reader.BlittableValidation;
                var message = Assert.Throws<FormatException>(() => test(reader.Size));
                Assert.Equal(message.Message, "Bad variable size int");
            }
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

            Employee[] employees = new Employee[10];
            for (int i = 0; i < 10; i++)
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

        public class Empty
        {

        }

        public class City
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string RegionId { get; set; }
            public string CountryId { get; set; }

        }

        public class Region
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CountryId { get; set; }
        }

        public class ContactClass
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }

        public class AddressClass
        {
            public string Line1 { get; set; }
            public string Line2 { get; set; }
            public City City { get; set; }
            public Region Region { get; set; }
            public int PostalCode { get; set; }
            public string Country { get; set; }
        }

        public class Company
        {
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public ContactClass Contact { get; set; }
            public AddressClass Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
            public CompanyType Type { get; set; }
            public Employee[] EmployeesIds { get; set; }

            public enum CompanyType
            {
                Public,
                Private
            }
        }

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }
    }
}
