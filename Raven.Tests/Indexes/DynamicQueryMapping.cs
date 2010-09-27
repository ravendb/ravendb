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
            var mapping = Data.DynamicQueryMapping.Create("Term:[0 TO 10]");
            Assert.Equal("Term", mapping.Items[0].From);
        }

        [Fact]
        public void CanExtractTermsFromEqualityQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever");
            Assert.Equal("Term", mapping.Items[0].From);
        }

        [Fact]
        public void CanExtractMultipleTermsQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever OR Term2:[0 TO 10]");

            Assert.Equal(2, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));    
        }

        [Fact]
        public void CanExtractTermsFromComplexQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("+(Term:bar Term2:baz) +Term3:foo -Term4:rob");
            Assert.Equal(4, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));
            Assert.True(mapping.Items.Any(x => x.From == "Term3"));
            Assert.True(mapping.Items.Any(x => x.From == "Term4"));
        }

        [Fact]
        public void CanExtractMultipleNestedTermsQuery()
        {
            var mapping = Data.DynamicQueryMapping.Create("Term:Whatever OR (Term2:Whatever AND Term3:Whatever)");
            Assert.Equal(3, mapping.Items.Length);
            Assert.True(mapping.Items.Any(x => x.From == "Term"));
            Assert.True(mapping.Items.Any(x => x.From == "Term2"));
            Assert.True(mapping.Items.Any(x => x.From == "Term3"));  
        }
    }
}
