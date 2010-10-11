using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Data=Raven.Database.Data;

namespace Raven.Tests.Indexes
{
    public class DynamicQueryMapping
    {
        [Fact]
        public void CanExtractTermsFromRangedQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:[0 TO 10]",null);
            Assert.Equal("Term", mapping.Items[0].From);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever", null);
            Assert.Equal("Term", mapping.Items[0].From);
        }

        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever OR Term2:[0 TO 10]", null);

            Assert.Equal(2, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));    
        }

        [Fact]
        public void CanExtractTermsFromComplexQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("+(Term:bar Term2:baz) +Term3:foo -Term4:rob", null);
            Assert.Equal(4, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));
            Assert.True(mapping.Items.Any(x => x.From == "Term3"));
            Assert.True(mapping.Items.Any(x => x.From == "Term4"));
        }

        [Fact]
        public void CanExtractMultipleNestedTermsQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever OR (Term2:Whatever AND Term3:Whatever)", null);
            Assert.Equal(3, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));
            Assert.True(mapping.Items.Any(x => x.From == "Term3"));  
        }

        [Fact]
        public void CreateDefinitionSupportsSimpleProperties()
        {
            Data.DynamicQueryMapping mapping = new Data.DynamicQueryMapping()
            {
                 Items = new Data.DynamicQueryMappingItem[]{
                        new Data.DynamicQueryMappingItem(){
                             From = "Name",
                             To = "Name"
                        }
                 }
            };

            var definition = mapping.CreateIndexDefinition();
            Assert.Equal("from doc in docs\r\nselect new { Name = doc.Name }", definition.Map);
                
        }

        [Fact]
        public void CreateDefinitionSupportsArrayProperties()
        {
            Data.DynamicQueryMapping mapping = new Data.DynamicQueryMapping()
            {
                Items = new Data.DynamicQueryMappingItem[]{
                        new Data.DynamicQueryMappingItem(){
                             From = "Tags,Name",
                             To = "docTagsName"
                        }
                 }
            };

            var definition = mapping.CreateIndexDefinition();
            Assert.Equal("from doc in docs\r\nfrom docTagsItem in doc.Tags\r\nselect new { docTagsName = docTagsItem.Name }", definition.Map);
        }

        [Fact]
        public void CreateDefinitionSupportsNestedProperties()
        {
            Data.DynamicQueryMapping mapping = new Data.DynamicQueryMapping()
            {
                Items = new Data.DynamicQueryMappingItem[]{
                        new Data.DynamicQueryMappingItem(){
                             From = "User.Name",
                             To = "UserName"
                        }
                 }
            };

            var definition = mapping.CreateIndexDefinition();
            Assert.Equal("from doc in docs\r\nselect new { UserName = doc.User.Name }", definition.Map);
        }
    }
}
