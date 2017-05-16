using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
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
            create_dynamic_mapping_for_users_collection("Term:[0 TO 10]");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Term"));
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever OR Term2:[0 TO 10]");

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
            create_dynamic_mapping_for_users_collection("+(Term:bar Term2:baz) +Term3:foo -Term4:rob");

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
            create_dynamic_mapping_for_users_collection("Term:Whatever OR (Term2:Whatever AND Term3:Whatever)");

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
            create_dynamic_mapping_for_users_collection("Tags,Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Tags,Name"));
            Assert.Equal("Auto/Users/ByTags_Name", definition.Name);
        }


        [Fact]
        public void CreateDefinitionSupportsNestedProperties()
        {
            create_dynamic_mapping_for_users_collection("User.Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("User.Name"));
            Assert.Equal("Auto/Users/ByUser_Name", definition.Name);
        }

        [Fact]
        public void CreateDefinitionForQueryWithSortedFields()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Name:A*",
                SortedFields = new[]
                {
                    new SortedField("Age_L_Range"),
                },
            });

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
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Name:A*",
                SortedFields = new[]
                {
                    new SortedField("Address.Country"),
                },
            });

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Address.Country"));
            Assert.Equal("Auto/Users/ByAddress_CountryAndName", definition.Name);
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.Sort);
            var ageField = definition.GetField("Address.Country");
            Assert.Equal(SortOptions.String, ageField.Sort);
        }

        [Fact]
        public void CreateDefinitionForQueryWithNestedFieldsAndNumberSortingSet()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Name:A*",
                SortedFields = new[]
                {
                    new SortedField("Address.ZipCode_D_Range"),
                },
            });

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Count);
            Assert.Equal("Users", definition.Collections.Single());
            Assert.True(definition.ContainsField("Name"));
            Assert.True(definition.ContainsField("Address.ZipCode"));
            Assert.Equal("Auto/Users/ByAddress_ZipCodeAndNameSortByAddress_ZipCode", definition.Name);
            var nameField = definition.GetField("Name");
            Assert.Null(nameField.Sort);
            var ageField = definition.GetField("Address.ZipCode");
            Assert.Equal(SortOptions.Numeric, ageField.Sort);
        }

        [Fact]
        public void CreateDefinitionForQueryWithRangeField()
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "Age_D_Range:{30 TO NULL}"
            });

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
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "FirstName:A*",
                SortedFields = new[]
                {
                    new SortedField("Count_L_Range"),
                },
            });

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "LastName:A*",
                SortedFields = new[]
                {
                    new SortedField("Age_D_Range"),
                },
            });

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
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "FirstName:A* LastName:a*",
                SortedFields = new[]
                {
                    new SortedField("Count_L_Range"),
                },
            });

            var existingDefinition = _sut.CreateAutoIndexDefinition();

            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "FirstName:A* AddressId:addresses/1",
                SortedFields = new[]
                {
                    new SortedField("Age_D_Range"),
                    new SortedField("Count_L_Range")
                },
            });

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

        private void create_dynamic_mapping_for_users_collection(string query)
        {
            _sut = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = query
            });
        }
    }
}