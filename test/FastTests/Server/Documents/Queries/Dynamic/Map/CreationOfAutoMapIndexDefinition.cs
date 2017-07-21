using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.Map
{
    public class CreationOfAutoMapIndexDefinition : NoDisposalNeeded
    {
        private DynamicQueryMapping _sut;

        [Fact]
        public void SpecifyingInvalidParametersWillResultInException()
        {
            var fields = new[] { new IndexField
            {
                Name = "test",
                Storage = FieldStorage.No
            } };

            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition(null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition("test", null));
            Assert.Throws<ArgumentNullException>(() => new AutoMapIndexDefinition(null, fields));

            Assert.Throws<ArgumentException>(() => new AutoMapIndexDefinition("test", new IndexField[0]));

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
            Assert.Equal("Auto/Users/ByTermSortByTerm", definition.Name);
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
            Assert.Equal("Auto/Users/ByTermAndTerm2SortByTerm2", definition.Name);
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
            Assert.Equal("Auto/Users/ByAgeAndNameSortByAge", definition.Name);
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.Sort);
            var ageField = definition.GetField("Age");
            Assert.Equal(SortOptions.Numeric, ageField.Sort);
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
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.Sort);
            var ageField = definition.GetField("Address.Country");
            Assert.Equal(SortOptions.String, ageField.Sort);
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
            Assert.Equal("Auto/Users/ByAddress.ZipCodeAndNameSortByAddress.ZipCode", definition.Name);
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.Sort);
            var ageField = definition.GetField("Address.ZipCode");
            Assert.Equal(SortOptions.Numeric, ageField.Sort);
        }

        [Fact]
        public void CreateDefinitionForQueryWithRangeField()
        {
            create_dynamic_mapping("FROM Users WHERE Age BETWEEN 30 AND 40 ORDER BY Age");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAgeSortByAge", definition.Name);
            var nameField = definition.GetField("Age");
            Assert.Equal(SortOptions.Numeric, nameField.Sort);
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
            Assert.Equal("Auto/Users/ByAgeAndCountAndFirstNameAndLastNameSortByAgeCount", definition.Name);

            var ageField = definition.GetField("Age");
            Assert.Equal(SortOptions.Numeric, ageField.Sort);

            var countField = definition.GetField("Count");
            Assert.Equal(SortOptions.Numeric, countField.Sort);
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
            Assert.Equal("Auto/Users/ByAddressIdAndAgeAndCountAndFirstNameAndLastNameSortByAgeCount", definition.Name);

            var ageField = definition.GetField("Age");
            Assert.Equal(SortOptions.Numeric, ageField.Sort);

            var countField = definition.GetField("Count");
            Assert.Equal(SortOptions.Numeric, countField.Sort);
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
            var nameField = definition.GetField("Age");
            Assert.Equal(SortOptions.String, nameField.Sort);
        }

        private void create_dynamic_mapping(string query)
        {
            _sut = DynamicQueryMapping.Create(new IndexQueryServerSide(query));
        }
    }
}