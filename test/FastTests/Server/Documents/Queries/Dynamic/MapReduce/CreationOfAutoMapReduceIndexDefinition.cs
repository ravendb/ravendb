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
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("SELECT Location, count() FROM Users GROUP BY Location"));

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByCountReducedByLocation", definition.Name);
        }

        [Fact]
        public void Error_when_no_group_by_field()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("SELECT count() FROM Users"));

            var ex = Assert.Throws<InvalidOperationException>(() => _sut.CreateAutoIndexDefinition());

            Assert.Contains("no group by field", ex.Message);
        }

        [Fact]
        public void Error_when_no_aggregation_field()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location"));

            var ex = Assert.Throws<InvalidOperationException>(() => _sut.CreateAutoIndexDefinition());

            Assert.Contains("no aggregation", ex.Message);
        }

        [Fact]
        public void Extends_mapping_based_on_existing_definition_if_group_by_fields_match()
        {
            _sut = DynamicQueryMapping.Create(
                new IndexQueryServerSide("SELECT Location, count() FROM Users GROUP BY Location WHERE StartsWith(Location, 'A') ORDER BY Count as long"));

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide(
                "SELECT Location, count(), sum(Age) FROM Users GROUP BY Location WHERE StartsWith(Location, 'A') ORDER BY Age as long"));

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
    }
}