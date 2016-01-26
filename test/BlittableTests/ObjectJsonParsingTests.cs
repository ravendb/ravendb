using System.Collections.Generic;
using Xunit;

namespace BlittableTests
{
    public class ObjectJsonParsingTests
    {
        [Fact]
        public void CanParseSimpleObject()
        {
            var doc = new DynamicJsonBuilder
            {
                ["Name"] = "Oren Eini"
            };
        } 
    }

}