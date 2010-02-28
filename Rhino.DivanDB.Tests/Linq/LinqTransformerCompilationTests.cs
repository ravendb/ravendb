using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using Newtonsoft.Json.Linq;
using Rhino.DivanDB.Json;
using Rhino.DivanDB.Linq;
using Xunit;

namespace Rhino.DivanDB.Tests.Linq
{
    public class LinqTransformerCompilationTests
    {
        const string query = @"
    from doc in docs
    where doc.Type == ""page""
    select new { Key = doc.Title, Value = doc.Content, Size = doc.Size };
";
        [Fact]
        public void Will_compile_query_successfully()
        {
            var compiled = new LinqTransformer("pagesByTitle", query, "docs", System.IO.Path.GetTempPath(), typeof(JsonDynamicObject)).CompiledType;
            Assert.NotNull(compiled);
        }

        [Fact]
        public void Can_create_new_instance_from_query()
        {
            var compiled = new LinqTransformer("pagesByTitle", query, "docs", System.IO.Path.GetTempPath(), typeof(JsonDynamicObject)).CompiledType;
            Activator.CreateInstance(compiled);
        }

        [Fact]
        public void Can_get_type_of_result_from_query()
        {
            var compiled = new LinqTransformer("pagesByTitle", query, "docs", System.IO.Path.GetTempPath(), typeof(JsonDynamicObject)).CompiledType;
            var instance = (AbstractViewGenerator)Activator.CreateInstance(compiled);
            var argument = instance.IndexDefinition.Body.Type.GetGenericArguments()[0];
            
            Assert.NotNull(argument.GetProperty("Key"));
            Assert.NotNull(argument.GetProperty("Value"));
            Assert.NotNull(argument.GetProperty("Size"));

            Assert.Equal(typeof(string), argument.GetProperty("Key").PropertyType);
            Assert.Equal(typeof(string), argument.GetProperty("Value").PropertyType);
            Assert.Equal(typeof(string), argument.GetProperty("Size").PropertyType);
        }

        [Fact]
        public void Can_execute_query()
        {
            var compiled = new LinqTransformer("pagesByTitle", query, "docs", System.IO.Path.GetTempPath(), typeof(JsonDynamicObject)).CompiledType;
            var generator = (AbstractViewGenerator)Activator.CreateInstance(compiled);
            var results = generator.Execute(new[]
            {
                new JsonDynamicObject(@"
                {
                    '_id': 1,
                    'Type': 'page',
                    'Title': 'doc1',
                    'Content': 'Foobar',
                    'Size': 31
                }"),
                new JsonDynamicObject(@"
                {
                    '_id': 2,
                    'Type': 'not a page',
                }"),
                new JsonDynamicObject(@"
                {
                    '_id': 3,
                    'Type': 'page',
                    'Title': 'doc2',
                    'Content': 'Foobar',
                    'Size': 31
                }"),
            }).Cast<object>().ToArray();

            var expected = new[]
            {
                "{ Key = doc1, Value = Foobar, Size = 31, _id = 1 }",
                "{ Key = doc2, Value = Foobar, Size = 31, _id = 3 }"
            };

            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(expected[i], results[i].ToString());
            }
        }
    }
}