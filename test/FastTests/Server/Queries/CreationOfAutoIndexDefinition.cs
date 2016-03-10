using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Queries.Dynamic;

using Xunit;

namespace FastTests.Server.Queries
{
    public class CreationOfAutoIndexDefinition
    {
        private DynamicQueryMapping _sut;

        [Fact]
        public void SpecifyingInvalidParametersWillResultInException()
        {
            var fields = new[] { new IndexField
            {
                Name = "test",
                Highlighted = false,
                Storage = FieldStorage.No
            } };

            Assert.Throws<ArgumentNullException>(() => new AutoIndexDefinition(null, null));
            Assert.Throws<ArgumentNullException>(() => new AutoIndexDefinition("test", null));
            Assert.Throws<ArgumentNullException>(() => new AutoIndexDefinition(null, fields));

            Assert.Throws<ArgumentException>(() => new AutoIndexDefinition("test", new IndexField[0]));

            new AutoIndexDefinition("test", fields);
        }

        [Fact]
        public void CanExtractTermsFromRangedQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:[0 TO 10]");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever OR Term2:[0 TO 10]");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.Equal("Auto/Users/ByTermAndTerm2", definition.Name);
        }


        [Fact]
        public void CanExtractTermsFromComplexQuery()
        {
            create_dynamic_mapping_for_users_collection("+(Term:bar Term2:baz) +Term3:foo -Term4:rob");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.True(definition.ContainsField("Term3"));
            Assert.True(definition.ContainsField("Term4"));
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3AndTerm4", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleNestedTermsQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever OR (Term2:Whatever AND Term3:Whatever)");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Term"));
            Assert.True(definition.ContainsField("Term2"));
            Assert.True(definition.ContainsField("Term3"));
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3", definition.Name);
        }

        [Fact]
        public void CreateDefinitionSupportsArrayProperties()
        {
            create_dynamic_mapping_for_users_collection("Tags,Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Tags,Name"));
            Assert.Equal("Auto/Users/ByTags_Name", definition.Name);
        }


        [Fact]
        public void CreateDefinitionSupportsNestedProperties()
        {
            create_dynamic_mapping_for_users_collection("User.Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("User.Name"));
            Assert.Equal("Auto/Users/ByUser_Name", definition.Name);
        }

        [Fact]
        public void CreateDefinitionForQueryWithSortedFields()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Name:A*",
                SortedFields = new[]
                {
                    new SortedField("Age_Range"), 
                },
            });

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAgeAndNameSortByAge", definition.Name);
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.SortOption);
            var ageField = definition.GetField("Age");
            Assert.Equal(SortOptions.NumericDefault, ageField.SortOption);
        }

        [Fact]
        public void CreateDefinitionForQueryWithRangeField()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "Age_Range:{Ix30 TO NULL}"
            });

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("Age"));
            Assert.Equal("Auto/Users/ByAgeSortByAge", definition.Name);
            var nameField = definition.GetField("Age");
            Assert.Equal(SortOptions.NumericDefault, nameField.SortOption);
        }

        [Fact]
        public void ExtendsMappingBasedOnExistingDefinition()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "FirstName:A*",
                SortedFields = new[]
                {
                    new SortedField("Count_Range"),
                },
            });

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = "LastName:A*",
                SortedFields = new[]
                {
                    new SortedField("Age_Range"),
                },
            });

            _sut.ExtendMappingBasedOn(existingDefinition);

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.True(definition.ContainsField("FirstName"));
            Assert.True(definition.ContainsField("LastName"));
            Assert.True(definition.ContainsField("Age"));
            Assert.True(definition.ContainsField("Count"));
            Assert.Equal("Auto/Users/ByAgeAndCountAndFirstNameAndLastNameSortByAgeCount", definition.Name);

            var ageField = definition.GetField("Age");
            Assert.Equal(SortOptions.NumericDefault, ageField.SortOption);

            var countField = definition.GetField("Count");
            Assert.Equal(SortOptions.NumericDefault, countField.SortOption);
        }

        private void create_dynamic_mapping_for_users_collection(string query)
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQuery
            {
                Query = query
            });
        }
    }
}