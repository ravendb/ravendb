using System.Linq;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Queries.Dynamic;

using Xunit;

namespace FastTests.Server.Queries
{
    public class CreationOfAutoIndexDefinition
    {
        private DynamicQueryMapping _sut;

        [Fact]
        public void CanExtractTermsFromRangedQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:[0 TO 10]");
            
            var definition = _sut.CreateAutoIndexDefinition();
            
            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Equal("Term", definition.MapFields.ToArray()[0]);
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Equal("Term", definition.MapFields.ToArray()[0]);
            Assert.Equal("Auto/Users/ByTerm", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever OR Term2:[0 TO 10]");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Contains("Term", definition.MapFields);
            Assert.Contains("Term2", definition.MapFields);
            Assert.Equal("Auto/Users/ByTermAndTerm2", definition.Name);
        }


        [Fact]
        public void CanExtractTermsFromComplexQuery()
        {
            create_dynamic_mapping_for_users_collection("+(Term:bar Term2:baz) +Term3:foo -Term4:rob");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Contains("Term", definition.MapFields);
            Assert.Contains("Term2", definition.MapFields);
            Assert.Contains("Term3", definition.MapFields);
            Assert.Contains("Term4", definition.MapFields);
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3AndTerm4", definition.Name);
        }


        [Fact]
        public void CanExtractMultipleNestedTermsQuery()
        {
            create_dynamic_mapping_for_users_collection("Term:Whatever OR (Term2:Whatever AND Term3:Whatever)");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Contains("Term", definition.MapFields);
            Assert.Contains("Term2", definition.MapFields);
            Assert.Contains("Term3", definition.MapFields);
            Assert.Equal("Auto/Users/ByTermAndTerm2AndTerm3", definition.Name);
        }

        [Fact]
        public void CreateDefinitionSupportsArrayProperties()
        {
            create_dynamic_mapping_for_users_collection("Tags,Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Contains("Tags,Name", definition.MapFields.ToArray()[0]);
            Assert.Equal("Auto/Users/ByTags_Name", definition.Name);
        }


        [Fact]
        public void CreateDefinitionSupportsNestedProperties()
        {
            create_dynamic_mapping_for_users_collection("User.Name:Any");

            var definition = _sut.CreateAutoIndexDefinition();

            Assert.Equal(1, definition.Collections.Length);
            Assert.Equal("Users", definition.Collections[0]);
            Assert.Contains("User.Name", definition.MapFields.ToArray()[0]);
            Assert.Equal("Auto/Users/ByUser_Name", definition.Name);
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