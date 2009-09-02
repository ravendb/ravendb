using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Linq;
using Xunit;

namespace Rhino.DivanDB.Tests
{
    public class LinqTransformerCompilationTests
    {
        const string query = @"
var pagesByTitle = 
    from doc in docs
    where doc.Type == ""page""
    select new { Key = doc.Title, Value = doc.Content, Size = (int)doc.Size };
";
        [Fact]
        public void Will_compile_query_successfully()
        {
            var compiled = new LinqTransformer(query, "docs", typeof(JsonDynamicObject)).Compile();
            Assert.NotNull(compiled);
        }

        [Fact]
        public void Can_create_new_instance_from_query()
        {
			var compiled = new LinqTransformer(query, "docs", typeof(JsonDynamicObject)).Compile();
            Activator.CreateInstance(compiled);
        }

        [Fact]
        public void Can_get_type_of_result_from_query()
        {
			var compiled = new LinqTransformer(query, "docs", typeof(JsonDynamicObject)).Compile();
			var instance = (AbstractViewGenerator<JsonDynamicObject>)Activator.CreateInstance(compiled);
            var argument = instance.ViewDefinition.Body.Type.GetGenericArguments()[0];
            
            Assert.NotNull(argument.GetProperty("Key"));
            Assert.NotNull(argument.GetProperty("Value"));
            Assert.NotNull(argument.GetProperty("Size"));

            Assert.Equal(typeof(string), argument.GetProperty("Key").PropertyType);
            Assert.Equal(typeof(string), argument.GetProperty("Value").PropertyType);
            Assert.Equal(typeof(int), argument.GetProperty("Size").PropertyType);
        }

        [Fact]
        public void Can_execute_query()
        {
			var compiled = new LinqTransformer(query, "docs", typeof(JsonDynamicObject)).Compile();
			var generator = (AbstractViewGenerator<JsonDynamicObject>)Activator.CreateInstance(compiled);
            var results = generator.Execute(new[]
            {
                new JsonDynamicObject(@"
                {
                    'Type': 'page',
                    'Title': 'doc1',
                    'Content': 'Foobar',
                    'Size': 31
                }"),
                new JsonDynamicObject(@"
                {
                    'Type': 'not a page',
                }"),
                new JsonDynamicObject(@"
                {
                    'Type': 'page',
                    'Title': 'doc2',
                    'Content': 'Foobar',
                    'Size': 31
                }"),
            }).Cast<object>().ToArray();

            var expected = new[]
            {
                "{ Key = doc1, Value = Foobar, Size = 31 }",
                "{ Key = doc2, Value = Foobar, Size = 31 }"
            };

            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expected[i], results[i].ToString());
            }
        }
    }
}
