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
        public class Blog
        {
            public string Title { get; set; }

            public string Category { get; set; }
        }

        [Fact]
        public void SimpleObjectWithStrings ()
        {
            var blog = new Blog();
            var document = new JsonDocument {  DataAsJson = JsonExtensions.ToJObject(blog) };

            var generator = new JsonCodeGenerator("csharp");
            var result = generator.Execute(document);
        }
    }
}
