using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Impl.Generators;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Json
{
    public class JsonCodeGeneratorTests : RavenTest
    {
        public class WithStrings
        {
            public string Title { get; set; }

            public string Category { get; set; }
        }

        public class WithIntsAndDateTimes
        {
            public int Integer { get; set; }
            public long Long { get; set; }
            public DateTime Date { get; set; }
        }

        public class WithInnerObject
        {
            public WithIntsAndDateTimes First { get; set; }
            public WithStrings Second { get; set; }
        }

        public class WithArrayOfBasics
        {
            public IList<int> Ints { get; set; }
            public IList<string> Strings { get; set; }
        }

        public class WithArrayOfObjects
        {
            public IList<WithStrings> Objects { get; set; }
        }

        [Fact]
        public void JsonCodeGenerator_SimpleObjectWithStrings()
        {
            var obj = new WithStrings() { Title = "test", Category = "category" };
            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithStrings", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(1, classTypes.Count());

            throw new NotImplementedException();
        }

        [Fact]
        public void JsonCodeGenerator_SimpleObjectWithNumericsAndDateTime()
        {
            var obj = new WithIntsAndDateTimes() { Integer = int.MaxValue, Long = long.MaxValue, Date = DateTime.Now };

            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithIntsAndDateTimes", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(1, classTypes.Count());

            throw new NotImplementedException();
        }

        [Fact]
        public void JsonCodeGenerator_ContentResolutionForNumerics()
        {
            // Result will be Int,Int,DateTime
            var obj = new WithIntsAndDateTimes() { Integer = 1, Long = 2, Date = DateTime.Now };

            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithIntsAndDateTimes", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(1, classTypes.Count());

            throw new NotImplementedException();
        }

        [Fact]
        public void JsonCodeGenerator_WithDistinctInnerObject()
        {
            var obj = new WithInnerObject()
            {
                First = new WithIntsAndDateTimes() { Integer = 1, Long = (long)(int.MaxValue) + 1, Date = DateTime.Now },
                Second = new WithStrings() { Title = "test", Category = "category" }
            };

            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithInnerObject", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(3, classTypes.Count());


            var clazz = classTypes["WithInnerObject"].Single() as JsonCodeGenerator.ClassType;
            Assert.NotNull(clazz);

            var first = classTypes["FirstClass"].Single() as JsonCodeGenerator.ClassType;
            Assert.NotNull(first);

            Assert.Equal(3, first.Properties.Count);
            Assert.Equal("int", first.Properties["Integer"].Name);
            Assert.Equal("long", first.Properties["Long"].Name);
            Assert.Equal("DateTimeOffset", first.Properties["Date"].Name);

            Assert.True(first.Properties.All(x => !x.Value.IsArray && x.Value.IsPrimitive));

            var second = classTypes["SecondClass"].Single() as JsonCodeGenerator.ClassType;
            Assert.NotNull(second);

            Assert.Equal(2, second.Properties.Count);
            Assert.Equal("string", second.Properties["Title"].Name);
            Assert.Equal("string", second.Properties["Category"].Name);
            Assert.True(second.Properties.All(x => !x.Value.IsArray && x.Value.IsPrimitive));
        }

        [Fact]
        public void JsonCodeGenerator_WithArrayOfBasics()
        {
            var obj = new WithArrayOfBasics()
            {
                Ints = new int[] { 0, 1, 2 },
                Strings = new string[] { "test", "category" }
            };

            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithArrayOfBasics", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(1, classTypes.Count());

            throw new NotImplementedException();
        }


        [Fact]
        public void JsonCodeGenerator_WithArrayOfObjects()
        {
            var obj = new WithArrayOfObjects()
            {
                Objects = new List<WithStrings> {
                     new WithStrings() { Title = "test", Category = "category" },
                     new WithStrings() { Title = "test", Category = "category" },
                 }
            };

            var generator = new JsonCodeGenerator("csharp");
            var classTypes = generator.GenerateClassTypesFromObject("WithArrayOfObjects", JsonExtensions.ToJObject(obj))
                                      .ToLookup(x => x.Name);

            Assert.Equal(1, classTypes.Count());

            throw new NotImplementedException();
        }
    }
}