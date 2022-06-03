using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    public class CreationOfAutoMapIndexDefinition : NoDisposalNeeded
    {
        public CreationOfAutoMapIndexDefinition(ITestOutputHelper output) : base(output)
        {
        }

        private DynamicQueryMapping _sut;

        [Fact]
        public void SpecifyingInvalidParametersWillResultInException()
        {
            var fields = new[] { new AutoIndexField
            {
                Name = "test",
                Id = 1,
                Storage = FieldStorage.No
            } };

            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition(null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition("test", null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition(null, fields));

            new AutoMapIndexDefinition("test", fields);
        }

        [Fact]
        public void CanExtractTermsFromRangedQuery()
        {
            create_dynamic_mapping("FROM Users WHERE Term BETWEEN 0 AND 10");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            create_dynamic_mapping("FROM Users WHERE Term = 'Whatever'");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            create_dynamic_mapping("FROM Users WHERE Term = 'Whatever' OR Term2 BETWEEN 0 AND 10");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.Equal("Auto/Users/ByTermAndTerm2", definition.Name);
        }


        [Fact]
        public void CanExtractTermsFromComplexQuery()
        {
            create_dynamic_mapping("FROM Users WHERE (Term = 'bar' OR Term2 = 'baz') OR Term3 = 'foo' OR NOT Term4 = 'rob'");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.True(definition.ContainsField("Term3"));
            Assert.True(definition.ContainsField("Term4"));
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3AndTerm4", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleNestedTermsQuery()
        {
            create_dynamic_mapping("FROM Users WHERE Term = 'Whatever' OR (Term2 = 'Whatever' AND Term3 = 'Whatever')");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.True(definition.ContainsField("Term3"));
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3", definition.Name);
        }

        [Fact]
        public void CreateDefinitionSupportsArrayProperties()
        {
            create_dynamic_mapping("FROM Users WHERE Tags[].Name = 'Any'");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Tags[].Name"));
            Assert.Equal("Auto/Users/ByTags[].Name", definition.Name);
        }


        [Fact]
        public void CreateDefinitionSupportsNestedProperties()
        {
            create_dynamic_mapping("FROM Users WHERE User.Name = 'Any'");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("User.Name"));
            Assert.Equal("Auto/Users/ByUser.Name", definition.Name);
        }

        [Fact]
        public void CreateDefinitionForQueryWithSortedFields()
        {
            create_dynamic_mapping("FROM Users WHERE StartsWith(Name, 'a') ORDER BY Age AS long");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAgeAndName", definition.Name);
            var nameField = definition.MapFields["Name"];
        }

        [Fact]
        public void CreateDefinitionForQueryWithNestedFieldsAndStringSortingSet()
        {
            create_dynamic_mapping("FROM Users WHERE StartsWith(Name, 'a') ORDER BY Address.Country ASC");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Address.Country"));
            Assert.Equal("Auto/Users/ByAddress.CountryAndName", definition.Name);
        }

        [Fact]
        public void CreateDefinitionForQueryWithNestedFieldsAndNumberSortingSet()
        {
            create_dynamic_mapping("FROM Users WHERE StartsWith(Name, 'a') ORDER BY Address.ZipCode AS double");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Address.ZipCode"));
            Assert.Equal("Auto/Users/ByAddress.ZipCodeAndName", definition.Name);
        }

        [Fact]
        public void CreateDefinitionForQueryWithRangeField()
        {
            create_dynamic_mapping("FROM Users WHERE Age BETWEEN 30 AND 40 ORDER BY Age");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAge", definition.Name);
        }

        [Fact]
        public void ExtendsMappingBasedOnExistingDefinition()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE StartsWith(FirstName, 'a') ORDER BY Count AS long"));

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE StartsWith(LastName, 'A') ORDER BY Age AS double"));

            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("FirstName"));
            Assert.True(definition.ContainsField("LastName"));
            Assert.True(definition.ContainsField("Age"));
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByAgeAndCountAndFirstNameAndLastName", definition.Name);
        }

        [Fact]
        public void DefinitionExtensionWontDuplicateFields()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE StartsWith(FirstName, 'A') AND StartsWith(LastName, 'A') ORDER BY Age AS double, Count AS long"));

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE StartsWith(FirstName, 'A') AND AddressId = 'addresses/1' ORDER BY Age AS double, Count AS long"));


            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("FirstName"));
            Assert.True(definition.ContainsField("LastName"));
            Assert.True(definition.ContainsField("AddressId"));
            Assert.True(definition.ContainsField("Age"));
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByAddressIdAndAgeAndCountAndFirstNameAndLastName", definition.Name);
        }

        [Fact]
        public void OrderingSpecifiedUsing_AS_AfterOrderBy()
        {
            create_dynamic_mapping("FROM Users WHERE Age > 40 ORDER BY Age AS string");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAge", definition.Name);
        }

        [Fact]
        public void ExtendsIndexingOptionsOfTheSameField()
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE FirstName = 'a'"));

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE search(FirstName, 'A')"));

            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("FirstName"));
            Assert.Equal(AutoFieldIndexing.Default | AutoFieldIndexing.Search, definition.MapFields["FirstName"].As<AutoIndexField>().Indexing);
            Assert.Equal("Auto/Users/BySearch(FirstName)", definition.Name);


            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users WHERE exact(FirstName = 'A')"));

            _sut.ExtendMappingBasedOn(definition);

            definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("FirstName"));
            Assert.Equal(AutoFieldIndexing.Default | AutoFieldIndexing.Search | AutoFieldIndexing.Exact, definition.MapFields["FirstName"].As<AutoIndexField>().Indexing);
            Assert.Equal("Auto/Users/BySearch(FirstName)AndExact(FirstName)", definition.Name);
        }

        private void create_dynamic_mapping(string query)
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide(query));
        }
    }
}
