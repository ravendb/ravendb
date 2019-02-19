using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Parser;
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
                new AutoIndexField()
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, null));

            new AutoMapReduceIndexDefinition("test", new[]
            {
                new AutoIndexField
                {
                    Name = "test",
                    Storage = FieldStorage.Yes
                },
            }, new[]
            {
                new AutoIndexField
                {
                    Name = "location",
                    Storage = FieldStorage.Yes
                }
            });
        }

        [Fact]
        public void Map_all_fields()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT Location, count() "));

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByCountReducedByLocation", definition.Name);
        }

        [Fact]
        public void Error_when_no_group_by_field()
        {
            var ex = Assert.Throws<QueryParser.ParseException>(() => new IndexQueryServerSide("FROM Users GROUP BY SELECT count() "));

            Assert.Contains("Unable to get field for GROUP BY", ex.Message);
        }

        [Fact]
        public void Can_be_no_aggregation_field_in_dynamic_group_by()
        {
            new IndexQueryServerSide("FROM Users GROUP BY Location");
        }

        [Fact]
        public void Extends_mapping_based_on_existing_definition_if_group_by_fields_match()
        {
            _sut = DynamicQueryMapping.Create(
                new IndexQueryServerSide("FROM Users GROUP BY Location WHERE StartsWith(Location, 'A') ORDER BY Count SELECT Location, count() "));

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide(
                "FROM Users GROUP BY Location WHERE StartsWith(Location, 'A') ORDER BY Age as long SELECT Location, count(), sum(Age) "));

            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = (AutoMapReduceIndexDefinition)_sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Count"));
            Assert.True(definition.ContainsField("Age"));
            Assert.True(definition.GroupByFields.ContainsKey("Location"));

            Assert.Equal("Auto/Users/ByAgeAndCountReducedByLocation", definition.Name);
        }
    }
}
