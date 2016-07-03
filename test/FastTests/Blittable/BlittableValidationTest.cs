using System;
using System.IO;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableValidationTest : RavenTestBase
    {
        private BlittableJsonReaderObject InitSimpleBlittable(out int size)
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new JsonOperationContext(pool))
            {
                var obj = RavenJObject.FromObject(new Employee
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
                var reader = context.Read(stream, "docs/1 ");
                size = reader.Size;
                return reader;
            }
        }

        [Fact]
        public void WithEscape()
        {

            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new JsonOperationContext(pool))
            {
                var obj = RavenJObject.FromObject(new Employee
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
                var reader = context.Read(stream, "docs/1 ");
                reader.BlittableValidation();
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

        public class Str
        {
            public string str { get; set; }
        }

        private byte[] GetBlittableWithExtraSpace()
        {
            return new byte[190]
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
            var region = new Region {CountryId = "3123456789", Id = "24682468", Name = "North"};
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

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        [Fact]
        public void Complex_Valid_Blittable()
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
                reader.BlittableValidation();
            }
        }

        [Fact]
        public void Empty_Valid_Blittable()
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
                var reader = new BlittableJsonReaderObject(ptr, 0x16, null);
                var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Float not valid");
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
            int size;
            var reader = InitSimpleBlittable(out size);
            var basePointer = reader.BasePointer;

            *(basePointer + size - 7) = 0x11;
            var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
            Assert.Equal(message.Message, "Properties names token not valid");

            *(basePointer + size - 7) = 0x50;
            message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
            Assert.Equal(message.Message, "Properties names token not valid");

            *(basePointer + size - 7) = 0x00;
            var messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
            Assert.Equal(messageArg.Message, "Illegal offset size");

            *(basePointer + size - 7) = 0x10;
            var listStart = size - 6;

            for (var i = 0; i < 2; i++)
            {
                var temp = basePointer[listStart + i];

                basePointer[listStart + i] = (byte)listStart;
                message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
                Assert.Equal(message.Message, "Properties names offset not valid");
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
            int size;
            var reader = InitSimpleBlittable(out size);
            var basePointer = reader.BasePointer;

            *(basePointer + size - 1) = 0x52;
            var message = Assert.Throws<InvalidDataException>(() => reader.BlittableValidation());
            Assert.Equal(message.Message, "Illegal root object");

            *(basePointer + size - 1) = 0x41;
            var messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
            Assert.Equal(messageArg.Message, "Illegal offset size");

            *(basePointer + size - 1) = 0x11;
            messageArg = Assert.Throws<ArgumentException>(() => reader.BlittableValidation());
            Assert.Equal(messageArg.Message, "Illegal offset size");
        }

        [Fact]
        public unsafe void Invalid_String()
        {
            var blittable = new byte[26]
            {
                0x08, 0x61, 0x62, 0x63, 0x64, 0x0a, 0x61, 0x62, 0x63, 0x01,
                0x04, 0x01, 0x0b, 0x00, 0x05, 0x04, 0x74, 0x65, 0x6d, 0x70,
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
            int size;
            var reader = InitSimpleBlittable(out size);
            reader.BlittableValidation();
        }

        [Fact]
        public void Valid_Blittable()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var context = new JsonOperationContext(pool))
            {
                var allTokens = new AllTokensTypes
                {
                    Bool = true,
                    Float = 123.4567F,
                    Int = 45679123,
                    IntArray = new[] {1, 2, 3},
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
                reader.BlittableValidation();
            }
        }

        [Fact]
        public void Valid_Compressed_String()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new JsonOperationContext(pool))
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

                    reader.BlittableValidation();
                }
            }
        }
    }
}