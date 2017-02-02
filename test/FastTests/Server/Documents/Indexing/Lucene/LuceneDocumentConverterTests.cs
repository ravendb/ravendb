using System.Collections.Generic;
using Lucene.Net.Documents;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Document = Raven.Server.Documents.Document;

namespace FastTests.Server.Documents.Indexing.Lucene
{
    public class LuceneDocumentConverterTests : RavenLowLevelTestBase
    {
        private LuceneDocumentConverter _sut;

        private readonly JsonOperationContext _ctx;
        private readonly ConcurrentSet<BlittableJsonReaderObject> _docs = new ConcurrentSet<BlittableJsonReaderObject>();
        private readonly ConcurrentSet<LazyStringValue> _lazyStrings = new ConcurrentSet<LazyStringValue>();

        public LuceneDocumentConverterTests()
        {
            _ctx = JsonOperationContext.ShortTermSingleUse();
        }

        [Fact]
        public void Returns_null_value_if_property_is_null()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = null
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(2, _sut.Document.GetFields().Count);
            Assert.Equal(Constants.NullValue, _sut.Document.GetField("Name").StringValue);
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Returns_empty_string_value_if_property_has_empty_string()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = string.Empty
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(2, _sut.Document.GetFields().Count);
            Assert.Equal(Constants.EmptyString, _sut.Document.GetField("Name").StringValue);
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_string_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Arek"
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(2, _sut.Document.GetFields().Count);
            Assert.NotNull(_sut.Document.GetField("Name"));
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Reuses_cached_document_instance()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc1 = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Arek"
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc1.Key, doc1, _ctx, out shouldSkip);

            Assert.Equal("Arek", _sut.Document.GetField("Name").ReaderValue.ReadToEnd());
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);

            var doc2 = create_doc(new DynamicJsonValue
            {
                ["Name"] = "Pawel"
            }, "users/2");

            _sut.SetDocument(doc2.Key, doc2, _ctx, out shouldSkip);

            Assert.Equal("Pawel", _sut.Document.GetField("Name").ReaderValue.ReadToEnd());
            Assert.Equal("users/2", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_numeric_fields()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Weight",
                    Highlighted = false,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.NumericDouble
                },
                new IndexField
                {
                    Name = "Age",
                    Highlighted = false,
                    Storage = FieldStorage.No,
                    SortOption = SortOptions.NumericDefault
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Weight"] = 70.1,
                ["Age"] = 25,
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(5, _sut.Document.GetFields().Count);
            Assert.NotNull(_sut.Document.GetField("Weight"));
            var weightNumeric = _sut.Document.GetFieldable("Weight_Range") as NumericField;
            Assert.NotNull(weightNumeric);
            Assert.Equal(70.1, weightNumeric.NumericValue);
            Assert.NotNull(_sut.Document.GetField("Age"));
            var ageNumeric = _sut.Document.GetFieldable("Age_Range") as NumericField;
            Assert.NotNull(ageNumeric);
            Assert.Equal(25L, ageNumeric.NumericValue);

            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_nested_string_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Address.City",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Address"] = new DynamicJsonValue
                {
                    ["City"] = "NYC"
                }
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(2, _sut.Document.GetFields().Count);
            Assert.Equal("NYC", _sut.Document.GetField("Address_City").ReaderValue.ReadToEnd());
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_string_value_nested_inside_collection()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Friends,Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Friends"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["Name"] = "Joe"
                    },
                    new DynamicJsonValue
                    {
                        ["Name"] = "John"
                    }
                }
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(4, _sut.Document.GetFields().Count);
            Assert.Equal(2, _sut.Document.GetFields("Friends_Name").Length);

            Assert.Equal("Joe", _sut.Document.GetFields("Friends_Name")[0].ReaderValue.ReadToEnd());
            Assert.Equal("John", _sut.Document.GetFields("Friends_Name")[1].ReaderValue.ReadToEnd());

            Assert.Equal("true", _sut.Document.GetField("Friends_Name_IsArray").StringValue);

            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_string_value_nested_inside_double_nested_collections()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Companies,Products,Name",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Companies"] = new DynamicJsonArray
                {
                    new DynamicJsonValue
                        {
                            ["Products"] = new DynamicJsonArray
                            {
                                new DynamicJsonValue
                                    {
                                        ["Name"] = "Headphones CX7"
                                    },
                                    new DynamicJsonValue
                                    {
                                        ["Name"] = "Keyboard AD3"
                                    }
                            }
                        },
                        new DynamicJsonValue
                        {
                            ["Products"] = new DynamicJsonArray
                            {
                                new DynamicJsonValue
                                {
                                    ["Name"] = "Optical Mouse V2"
                                }
                            }
                        },
                }
            }, "companies/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(5, _sut.Document.GetFields().Count);
            Assert.Equal(3, _sut.Document.GetFields("Companies_Products_Name").Length);

            Assert.Equal("Headphones CX7", _sut.Document.GetFields("Companies_Products_Name")[0].ReaderValue.ReadToEnd());
            Assert.Equal("Keyboard AD3", _sut.Document.GetFields("Companies_Products_Name")[1].ReaderValue.ReadToEnd());
            Assert.Equal("Optical Mouse V2", _sut.Document.GetFields("Companies_Products_Name")[2].ReaderValue.ReadToEnd());

            Assert.Equal("true", _sut.Document.GetField("Companies_Products_Name_IsArray").StringValue);

            Assert.Equal("companies/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_complex_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Address",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
                new IndexField
                {
                    Name = "ResidenceAddress",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Address"] = new DynamicJsonValue
                {
                    ["City"] = "New York City"
                },
                ["ResidenceAddress"] = new DynamicJsonValue
                {
                    ["City"] = "San Francisco"
                }
            }, "users/1");

            bool shouldSkip;
            using (_sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip))
            {

                Assert.Equal(5, _sut.Document.GetFields().Count);
                Assert.Equal(@"{""City"":""New York City""}", _sut.Document.GetField("Address").StringValue);
                Assert.Equal("true", _sut.Document.GetField("Address" + LuceneDocumentConverterBase.ConvertToJsonSuffix).StringValue);
                Assert.Equal(@"{""City"":""San Francisco""}", _sut.Document.GetField("ResidenceAddress").StringValue);
                Assert.Equal("true", _sut.Document.GetField("ResidenceAddress" + LuceneDocumentConverterBase.ConvertToJsonSuffix).StringValue);
                Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);

                doc = create_doc(new DynamicJsonValue
                {
                    ["Address"] = new DynamicJsonValue
                    {
                        ["City"] = "NYC"
                    },
                    ["ResidenceAddress"] = new DynamicJsonValue
                    {
                        ["City"] = "Washington"
                    }
                }, "users/2");

                _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

                Assert.Equal(5, _sut.Document.GetFields().Count);
                Assert.Equal(@"{""City"":""NYC""}", _sut.Document.GetField("Address").StringValue);
                Assert.Equal("true", _sut.Document.GetField("Address" + LuceneDocumentConverterBase.ConvertToJsonSuffix).StringValue);
                Assert.Equal(@"{""City"":""Washington""}", _sut.Document.GetField("ResidenceAddress").StringValue);
                Assert.Equal("true", _sut.Document.GetField("ResidenceAddress" + LuceneDocumentConverterBase.ConvertToJsonSuffix).StringValue);
                Assert.Equal("users/2", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
            }
        }


        [Fact]
        public void Conversion_of_array_value()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Friends",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Friends"] = new DynamicJsonArray()
                {
                    "Dave", "James"
                }
            }, "users/1");

            bool shouldSkip;
            _sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip);

            Assert.Equal(4, _sut.Document.GetFields().Count);
            Assert.Equal("Dave", _sut.Document.GetFields("Friends")[0].ReaderValue.ReadToEnd());
            Assert.Equal("James", _sut.Document.GetFields("Friends")[1].ReaderValue.ReadToEnd());
            Assert.Equal("true", _sut.Document.GetField("Friends" + LuceneDocumentConverterBase.IsArrayFieldSuffix).StringValue);
            Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
        }

        [Fact]
        public void Conversion_of_array_having_complex_values()
        {
            _sut = new LuceneDocumentConverter(new IndexField[]
            {
                new IndexField
                {
                    Name = "Addresses",
                    Highlighted = false,
                    Storage = FieldStorage.No
                },
            });

            var doc = create_doc(new DynamicJsonValue
            {
                ["Addresses"] = new DynamicJsonArray()
                {
                    new DynamicJsonValue
                    {
                        ["City"] = "New York City"
                    },
                    new DynamicJsonValue
                    {
                        ["City"] = "NYC"
                    }
                }
            }, "users/1");

            bool shouldSkip;
            using (_sut.SetDocument(doc.Key, doc, _ctx, out shouldSkip))
            {
                Assert.Equal(5, _sut.Document.GetFields().Count);
                Assert.Equal(@"{""City"":""New York City""}", _sut.Document.GetFields("Addresses")[0].StringValue);
                Assert.Equal(@"{""City"":""NYC""}", _sut.Document.GetFields("Addresses")[1].StringValue);
                Assert.Equal("true", _sut.Document.GetField("Addresses" + LuceneDocumentConverterBase.ConvertToJsonSuffix).StringValue);
                Assert.Equal("true", _sut.Document.GetField("Addresses" + LuceneDocumentConverterBase.IsArrayFieldSuffix).StringValue);
                Assert.Equal("users/1", _sut.Document.GetField(Constants.Indexing.Fields.DocumentIdFieldName).StringValue);
            }
        }

        public Document create_doc(DynamicJsonValue document, string id)
        {
            var data = _ctx.ReadObject(document, id);

            _docs.Add(data);

            //_lazyStrings.
            var lazyStringValueRegular = _ctx.GetLazyString(id);
            var lazyStringValueLowerCase = _ctx.GetLazyString(id.ToLowerInvariant());

            return new Document
            {
                Data = data,
                Key = lazyStringValueRegular,
                LoweredKey = lazyStringValueLowerCase
            };
        }



        public override void Dispose()
        {
            foreach (var docReader in _docs)
            {
                docReader.Dispose();
            }

            _ctx.Dispose();

            base.Dispose();
        }
    }
}