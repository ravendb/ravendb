using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class LargeObjectsWithJsonTextReader : RavenTestBase
    {
        private class Data
        {
            public byte[] Foo { get; set; }	
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(17)]
        [InlineData(12337)]
        [InlineData(1024*1024*3+2381)]
        [InlineData(1024 * 1024 * 8)]
        public void ReadSingleValueAsStreamShouldWork(int size)
        {
            var random = new Random(size);

            var buffer = new byte[size];
            random.NextBytes(buffer);
            var d = new Data
            {
                Foo = buffer
            };

            var jsonObj = RavenJToken.FromObject(d);
            
            var jsonObjAsString = jsonObj.ToString();

            using (var textReader = new StringReader(jsonObjAsString))
            using (var jsonReader = new JsonTextReader(textReader))
            {
                jsonReader.Read(); // start object
                jsonReader.Read(); // property name
                var actual = new byte[buffer.Length];
                using (var objectStream = jsonReader.ReadBytesAsStream())
                {
                    objectStream.Read(actual, 0, actual.Length);

                    for (int a = 0; a < actual.Length; a++)
                    {
                        if (buffer[a] != actual[a])
                        {
                            Assert.True(false, "not equal on byte " + a);
                        }
                    }
                }

            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(17)]
        [InlineData(18)]
        [InlineData(19)]
        [InlineData(128)]
        [InlineData(12337)]
        [InlineData(1024 * 1024)]
        public void ReadBytesAsStreamShouldPreserveJsonTextReaderInternalState(int size)
        {
            var random = new Random(size);

            var buffer = new byte[size];			
            var items = new Data[3];
            for (int i = 0; i < 3; i++)
            {
                random.NextBytes(buffer);
                items[i] = new Data
                {
                    Foo = buffer.ToArray()
                };
            }

            var jsonObj = RavenJToken.FromObject(items);

            var jsonObjAsString = jsonObj.ToString();

            using (var textReader = new StringReader(jsonObjAsString))
            using (var jsonReader = new JsonTextReader(textReader))
            {
                jsonReader.Read(); //start array

                jsonReader.Read();
                jsonReader.Read(); // property name
                var actual = new byte[buffer.Length];
                buffer = items[0].Foo;
                using (var objectStream = jsonReader.ReadBytesAsStream())
                {
                    objectStream.Read(actual, 0, actual.Length); 

                    for (int a = 0; a < actual.Length; a++)
                    {
                        if (buffer[a] != actual[a])
                        {
                            Assert.True(false, "not equal on byte " + a);
                        }
                    }
                }

                jsonReader.Read(); //end object

                jsonReader.Read(); //start object
                jsonReader.Read(); // property name				
                actual = jsonReader.ReadAsBytes(); //	value;
                buffer = items[1].Foo;
                for (int a = 0; a < actual.Length; a++)
                {
                    if (buffer[a] != actual[a])
                    {
                        Assert.True(false, "not equal on byte " + a);
                    }
                }

                jsonReader.Read(); //end object


                jsonReader.Read(); //start object
                jsonReader.Read(); // property name
                actual = new byte[buffer.Length];
                buffer = items[2].Foo;
                using (var objectStream = jsonReader.ReadBytesAsStream())
                {
                    objectStream.Read(actual, 0, actual.Length); 

                    for (int a = 0; a < actual.Length; a++)
                    {
                        if (buffer[a] != actual[a])
                        {
                            Assert.True(false, "not equal on byte " + a);
                        }
                    }
                }

                jsonReader.Read(); //end object

            }
        }

        [Theory]
        [InlineData(1, 5)]
        [InlineData(2, 5)]
        [InlineData(3, 5)]
        [InlineData(4, 5)]
        [InlineData(5, 5)]
        [InlineData(6, 5)]
        [InlineData(7, 5)]
        [InlineData(16, 17)]
        [InlineData(1, 12337)]
        [InlineData(4, 12337)]
        [InlineData(1, 234234)]
        [InlineData(4, 234234)]
        [InlineData(26, 234238)]
        [InlineData(1, 65537)]
        [InlineData(4, 65537)]
        [InlineData(3, 128 * 1024)]
        [InlineData(1, 1024 * 1024 * 3 + 2381)]
        [InlineData(4, 1024 * 1024 * 3 + 2381)]
        [InlineData(1, 1024 * 1024 * 10)]
        [InlineData(4, 1024 * 1024 * 10)]
        public void ReadMultipleValuesAsStreamShouldWork(int itemsCount,int size)
        {
            var random = new Random(size);
            var items = new Data[itemsCount];
            var buffer = new byte[size];
            for (int i = 0; i < itemsCount; i++)
            {
                random.NextBytes(buffer);
                items[i] = new Data
                {
                    Foo = buffer.ToArray()
                };
            }

            var jsonObj = RavenJToken.FromObject(items);

            var jsonObjAsString = jsonObj.ToString();

            using (var textReader = new StringReader(jsonObjAsString))
            using (var jsonReader = new JsonTextReader(textReader))
            {
                jsonReader.Read(); // start array

                for (int i = 0; i < itemsCount; i++)
                {
                    jsonReader.Read();
                    jsonReader.Read(); // property name
                    var actual = new byte[buffer.Length];
                    buffer = items[i].Foo;
                    using (var objectStream = jsonReader.ReadBytesAsStream())
                    {
                        objectStream.Read(actual, 0, actual.Length);

                        //Assert.Equal(buffer, actual);
                        for (int a = 0; a < actual.Length; a++)
                        {
                            if (buffer[a] != actual[a])
                            {
                                Assert.True(false, "not equal on byte " + a);
                            }
                        }
                    }

                    jsonReader.Read(); //end object
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(10)]
        [InlineData(128)]
        [InlineData(1024 * 1024 * 3)]
        [InlineData(1024 * 1024 * 50)]
        public void SingleAttachmentsImportShouldWork(int size)
        {
            var data = new byte[size];
            new Random().NextBytes(data);

            using (var source = NewRemoteDocumentStore(databaseName: "fooDB"))
            {
                source.DatabaseCommands.ForSystemDatabase().CreateDatabase(new DatabaseDocument
                {
                    Id = "fooDB2",
                    Settings = new Dictionary<string, string>
                    {
                        {"Raven/DataDir", "FooData" }
                    }
                });

                using (var stream = new MemoryStream(data))
                {
                    stream.Position = 0;
                    source.DatabaseCommands.PutAttachment("foo", null, stream, new RavenJObject());
                }

                var exportSmugglerApi = new SmugglerApi(new SmugglerOptions
                {
                    OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                }, new RavenConnectionStringOptions
                {
                    Url = source.Url,
                    DefaultDatabase = "fooDB"
                });

                var importSmugglerApi = new SmugglerApi(new SmugglerOptions
                {
                    OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                }, new RavenConnectionStringOptions
                {
                    Url = source.Url,
                    DefaultDatabase = "fooDB2"
                });

                var filename = "large-attachment-test.ravendump";
                using (var fs = new FileStream(filename, FileMode.Create))
                {
                    exportSmugglerApi.ExportData(fs, new SmugglerOptions
                    {
                        OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                    }, false).Wait();
                    fs.Flush();
                }

                using (var fs = new FileStream(filename, FileMode.Open))
                {
                    importSmugglerApi.ImportData(fs,new SmugglerOptions
                    {
                        OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                    }).Wait();
                }

                var attachment = source.DatabaseCommands.ForDatabase("fooDB2").GetAttachment("foo");
                Assert.Equal(attachment.Data().ReadData(), data);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(10)]
        [InlineData(128)]
        [InlineData(1024 * 1024)]
        [InlineData(1024 * 1024 * 3)]
        public void MultipleAttachmentsImportShouldWork(int size)
        {
            const int ItemCount = 5;
            var buffer = new byte[size];
            var random = new Random(size);
            var dataList = new List<Data>();
            for (int i = 0; i < ItemCount; i++)
            {
                random.NextBytes(buffer);
                dataList.Add(new Data
                {
                    Foo = buffer.ToArray()
                });
            }

            using (var source = NewRemoteDocumentStore(databaseName: "fooDB"))
            {
                source.DatabaseCommands.ForSystemDatabase().CreateDatabase(new DatabaseDocument
                {
                    Id = "fooDB2",
                    Settings = new Dictionary<string, string>
                    {
                        {"Raven/DataDir", "FooData" }
                    }
                });

                int id = 1;
                foreach (var dataItem in dataList)
                {
                    using (var stream = new MemoryStream(dataItem.Foo))
                    {
                        stream.Position = 0;
                        source.DatabaseCommands.PutAttachment("foo/" + (id++), null, stream, new RavenJObject());
                    }
                }

                var exportSmugglerApi = new SmugglerApi(new SmugglerOptions
                {
                    OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                }, new RavenConnectionStringOptions
                {
                    Url = source.Url,
                    DefaultDatabase = "fooDB"
                });

                var importSmugglerApi = new SmugglerApi(new SmugglerOptions
                {
                    OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                }, new RavenConnectionStringOptions
                {
                    Url = source.Url,
                    DefaultDatabase = "fooDB2"
                });

                var filename = "large-attachment-test.ravendump";
                using (var fs = new FileStream(filename, FileMode.Create))
                {
                    exportSmugglerApi.ExportData(fs, new SmugglerOptions
                    {
                        OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                    }, false).Wait();
                    fs.Flush();
                }

                using (var fs = new FileStream(filename, FileMode.Open))
                {
                    importSmugglerApi.ImportData(fs, new SmugglerOptions
                    {
                        OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Attachments | ItemType.Transformers
                    }).Wait();
                }

                id = 1;
                foreach (var dataItem in dataList)
                {
                    var attachment = source.DatabaseCommands.ForDatabase("fooDB2").GetAttachment("foo/" + (id++));
                    Assert.Equal(attachment.Data().ReadData(), dataItem.Foo);
                }
            }
        }
    }
}
