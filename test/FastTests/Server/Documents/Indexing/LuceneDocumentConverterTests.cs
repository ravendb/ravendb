using System;
using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.Persistance.Lucene.Documents;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Xunit;
using Document = Raven.Server.Documents.Document;

namespace FastTests.Server.Documents.Indexing
{
    public class LuceneDocumentConverterTests : IDisposable
    {
        private LuceneDocumentConverter _sut;

        private readonly UnmanagedBuffersPool _pool;
        private readonly MemoryOperationContext _ctx;
        private readonly List<BlittableJsonReaderObject> _docs = new List<BlittableJsonReaderObject>();

        public LuceneDocumentConverterTests()
        {
            _pool = new UnmanagedBuffersPool("foo");
            _ctx = new MemoryOperationContext(_pool);
        }

        [Fact]
        public void Returns_null_value_if_property_is_not_present_in_document()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Name"),
            });

            var doc = create_doc(new DynamicJsonValue
            {
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);

            Assert.Equal(2, result.GetFields().Count);
            Assert.Equal(Constants.NullValue, result.GetField("Name").StringValue);
            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Returns_empty_string_value_if_property_has_empty_string()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Name"),
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = string.Empty
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);

            Assert.Equal(2, result.GetFields().Count);
            Assert.Equal(Constants.EmptyString, result.GetField("Name").StringValue);
            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_string_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Name"),
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Arek"
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);

            Assert.Equal(2, result.GetFields().Count);
            Assert.NotNull(result.GetField("Name"));
            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Reuses_cached_document_instance()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Name"),
            });

            var doc1 = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Arek"
            }, "users/1");

            var result1 = _sut.ConvertToCachedDocument(doc1);
            
            Assert.Equal("Arek", result1.GetField("Name").ReaderValue.ReadToEnd());
            Assert.Equal("users/1", result1.GetField(Constants.DocumentIdFieldName).StringValue);

            var doc2 = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Pawel"
            }, "users/2");

            var result2 = _sut.ConvertToCachedDocument(doc2);

            Assert.Equal("Pawel", result2.GetField("Name").ReaderValue.ReadToEnd());
            Assert.Equal("users/2", result2.GetField(Constants.DocumentIdFieldName).StringValue);

            Assert.Same(result1, result2);
        }

        [Fact]
        public void Conversion_of_numeric_fields()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Weight", SortOptions.Double),
                new AutoIndexField("Age", SortOptions.Int),
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Weight"] = 70.1,
                ["Age"] = 25,
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);

            Assert.Equal(5, result.GetFields().Count);
            Assert.NotNull(result.GetField("Weight"));
            var weightNumeric = result.GetFieldable("Weight_Range") as NumericField;
            Assert.NotNull(weightNumeric);
            Assert.Equal(70.1, weightNumeric.NumericValue);
            Assert.NotNull(result.GetField("Age"));
            var ageNumeric = result.GetFieldable("Age_Range") as NumericField;
            Assert.NotNull(ageNumeric);
            Assert.Equal(25, ageNumeric.NumericValue);

            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_nested_string_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Address.City"),
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Address"] = new DynamicJsonValue
                {
                    ["City"] = "NYC"
                }
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);

            Assert.Equal(2, result.GetFields().Count);
            Assert.Equal("NYC", result.GetField("Address_City").ReaderValue.ReadToEnd());
            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        [Fact(Skip = "Currently failing")]
        public void Conversion_of_string_value_nested_inside_collection()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new AutoIndexField("Friends,Name"),
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Friends"] = new DynamicJsonArray
                {
                    Items = new Queue<object>(new[]
                    {
                        new DynamicJsonValue
                        {
                            ["Name"] = "Joe"
                        },
                        new DynamicJsonValue
                        {
                            ["Name"] = "John"
                        }
                    })
                }
            }, "users/1");

            var result = _sut.ConvertToCachedDocument(doc);
            
            Assert.Equal(4, result.GetFields().Count);
            Assert.Equal(2, result.GetFields("Friends_Name").Length);
            // TODO: Fix this test - because we are using a single LazyStringReader per 
            // field name, if we have two fields with the same name, they have the same reader instance
            // so they actually get the same name. In 3.x we handle that by also adding an internal 
            // index for the field name cache key
            Assert.Equal("Joe", result.GetFields("Friends_Name")[0].ReaderValue.ReadToEnd());
            Assert.Equal("John", result.GetFields("Friends_Name")[1].ReaderValue.ReadToEnd());

            Assert.Equal("true", result.GetField("Friends_Name_IsArray").StringValue);

            Assert.Equal("users/1", result.GetField(Constants.DocumentIdFieldName).StringValue);
        }

        public Document create_doc(DynamicJsonValue document, string id)
        {
            var data = _ctx.ReadObject(document, id);

            _docs.Add(data);

            return new Document
            {
                Data = data,
                Key = _ctx.GetLazyString(id)
            };
        }

        public void Dispose()
        {
            foreach (var docReader in _docs)
            {
                docReader.Dispose();
            }

            _ctx.Dispose();
            _pool.Dispose();
        }
    }
}