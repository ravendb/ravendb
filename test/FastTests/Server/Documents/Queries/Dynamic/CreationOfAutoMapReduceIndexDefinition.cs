using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic
{
    public class CreationOfAutoMapReduceIndexDefinition
    {
        private DynamicQueryMapping _sut;

        [Fact]
        public void SpecifyingInvalidParametersWillResultInException()
        {
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition(null, null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition(new[] { "test" }, null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition(new[] { "test" }, new[]
            {
                new IndexField()
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, null));

            Assert.Throws<ArgumentException>(() => new AutoMapReduceIndexDefinition(new[] { "test" }, new[]
            {
                new IndexField
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, new[]
            {
                new IndexField
                {
                    Name = "location",
                    Storage = FieldStorage.No
                }
            }));

            Assert.Throws<ArgumentException>(() => new AutoMapReduceIndexDefinition(new[] { "test" }, new[]
            {
                new IndexField
                {
                    Name = "test",
                    Storage = FieldStorage.No
                },
            }, new[]
            {
                new IndexField
                {
                    Name = "location",
                    Storage = FieldStorage.Yes
                }
            }));

            new AutoMapReduceIndexDefinition(new[] { "test" }, new[]
            {
                new IndexField
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, new[]
            {
                new IndexField
                {
                    Name = "location",
                    Storage = FieldStorage.Yes
                }
            });
        }

        [Fact]
        public void Map_all_fields()
        {
            create_dynamic_map_reduce_mapping_for_users_collection("", new[]
            {
                new DynamicMapReduceField
                {
                    Name = "Location",
                    IsGroupBy = true
                },
                new DynamicMapReduceField
                {
                    Name = "Count",
                    OperationType = FieldMapReduceOperation.Count
                }
            });

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByCountReducedByLocation", definition.Name);
        }

        [Fact]
        public void Error_when_no_group_by_field()
        {
            create_dynamic_map_reduce_mapping_for_users_collection("", new[]
            {
                new DynamicMapReduceField
                {
                    Name = "Count",
                    OperationType = FieldMapReduceOperation.Count
                }
            });

            var ex = Assert.Throws<InvalidOperationException>(() => _sut.CreateAutoIndexDefinition());

            Assert.Contains("no group by field", ex.Message);
        }

        [Fact]
        public void Error_when_no_aggregation_field()
        {
            create_dynamic_map_reduce_mapping_for_users_collection("", new[]
            {
                new DynamicMapReduceField
                {
                    Name = "Location",
                    IsGroupBy = true
                }
            });

            var ex = Assert.Throws<InvalidOperationException>(() => _sut.CreateAutoIndexDefinition());

            Assert.Contains("no aggregation", ex.Message);
        }

        private void create_dynamic_map_reduce_mapping_for_users_collection(string query, DynamicMapReduceField[] mapReduceFields)
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = query,
                DynamicMapReduceFields = mapReduceFields
            });
        }
    }
}