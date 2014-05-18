//-----------------------------------------------------------------------
// <copyright file="PerformingQueries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Json;
using Raven.Database.Linq;
using Raven.Database.Plugins;

using Xunit;

namespace Raven.Tests.Linq
{
    public class PerformingQueries : IDisposable
    {
        private const string query =
            @"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
";

        [Fact]
        public void Can_query_json()
        {
            var documents =
                GetDocumentsFromString(
                    @"[
{'type':'page', title: 'hello', content: 'foobar', size: 2, '@metadata': {'@id': 1}},
{'type':'page', title: 'there', content: 'foobar 2', size: 3, '@metadata': {'@id': 2} },
{'type':'revision', size: 4, _id: 3}
]");
            var transformer = new DynamicViewCompiler("pagesByTitle", new IndexDefinition { Map = query }, ".");
            var compiledQuery = transformer.GenerateInstance();
            var actual = compiledQuery.MapDefinitions[0](documents)
                .Cast<object>().ToArray();
            var expected = new[]
			{
				"{ Key = hello, Value = foobar, Size = 2, __document_id = 1 }",
				"{ Key = there, Value = foobar 2, Size = 3, __document_id = 2 }"
			};

            Assert.Equal(expected.Length, actual.Length);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], actual[i].ToString());
            }
        }

        [Fact]
        public void Can_extend_queries()
        {
            var documents =
                GetDocumentsFromString(
                    @"[{loc: 4, lang: 3, '@metadata': {'@id': 1}}]");
            var transformer = new DynamicViewCompiler("pagesByTitle", new IndexDefinition
            {
                Map = @"
from doc in docs
select new { GeoHash = PerformingQueries.SampleGeoLocation.GeoHash(doc.loc, doc.lang) }
"
            },
                                                      new OrderedPartCollection<AbstractDynamicCompilationExtension>
													  {
													  	new SampleDynamicCompilationExtension()
													  }, ".", new InMemoryRavenConfiguration());
            var compiledQuery = transformer.GenerateInstance();
            var actual = compiledQuery.MapDefinitions[0](documents)
                .Cast<object>().ToArray();
            var expected = new[]
			{
				"{ GeoHash = 4#3, __document_id = 1 }",
			};

            Assert.Equal(expected.Length, actual.Length);
            for (var i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], actual[i].ToString());
            }
        }

        public static IEnumerable<dynamic> GetDocumentsFromString(string json)
        {
            return RavenJArray.Parse(json).Cast<RavenJObject>().Select(JsonToExpando.Convert);
        }

        public class SampleDynamicCompilationExtension : AbstractDynamicCompilationExtension
        {
            public override string[] GetNamespacesToImport()
            {
                return new[]
                {
                    typeof (SampleGeoLocation).Namespace
                };
            }

            public override string[] GetAssembliesToReference()
            {
                return new[]
                {
                    typeof (SampleGeoLocation).Assembly.Location
                };
            }
        }

        public static class SampleGeoLocation
        {
            public static string GeoHash(int lon, int lang)
            {
                return lon + "#" + lang;
            }
        }

	    public void Dispose()
	    {
	    }
    }
}
