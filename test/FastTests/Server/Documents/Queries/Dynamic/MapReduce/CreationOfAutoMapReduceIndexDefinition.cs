using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class CreationOfAutoMapReduceIndexDefinition : NoDisposalNeeded
    {
        private DynamicQueryMapping _sut;

        [Fact]
        public void SpecifyingInvalidParametersWillResultInException()
        {
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition(null, null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition("test", null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapReduceIndexDefinition("test", new[]
            {
                new IndexField()
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, null));

            Assert.Throws<ArgumentException>(() => new AutoMapReduceIndexDefinition("test", new[]
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

            Assert.Throws<ArgumentException>(() => new AutoMapReduceIndexDefinition("test", new[]
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

            new AutoMapReduceIndexDefinition("test", new[]
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

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
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

        [Fact]
        public void Extends_mapping_based_on_existing_definition_if_group_by_fields_match()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Location:A*",
                //DynamicMapReduceFields = new[]
                //{
                //    new DynamicMapReduceField
                //    {
                //        Name = "Count",
                //        OperationType = FieldMapReduceOperation.Count
                //    },
                //    new DynamicMapReduceField
                //    {
                //        Name = "Location",
                //        IsGroupBy = true
                //    }
                //},
                SortedFields = new[]
                {
                    new SortedField("Count_L_Range"),
                }
            });

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Location:A*",
                //DynamicMapReduceFields = new[]
                //{
                //    new DynamicMapReduceField
                //    {
                //        Name = "Count",
                //        OperationType = FieldMapReduceOperation.Count,
                //    },
                //    new DynamicMapReduceField
                //    {
                //        Name = "Age",
                //        OperationType = FieldMapReduceOperation.Sum
                //    },
                //    new DynamicMapReduceField
                //    {
                //        Name = "Location",
                //        IsGroupBy = true
                //    }
                //},
                SortedFields = new[]
                {
                    new SortedField("Age_L_Range"),
                }
            });

            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = (AutoMapReduceIndexDefinition)_sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Count"));
            Assert.True(definition.ContainsField("Age"));
            Assert.True(definition.GroupByFields.ContainsKey("Location"));

            Assert.Equal(SortOptions.Numeric, definition.GetField("Count").Sort);
            Assert.Equal(SortOptions.Numeric, definition.GetField("Age").Sort);

            Assert.Equal("Auto/Users/ByAgeAndCountSortByAgeCountReducedByLocation", definition.Name);
        }

        private void create_dynamic_map_reduce_mapping_for_users_collection(string query, DynamicMapReduceField[] mapReduceFields)
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = query,
                //DynamicMapReduceFields = mapReduceFields
            });
        }
    }
}