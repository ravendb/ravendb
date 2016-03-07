using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Imports.Newtonsoft.Json.Sample
{
    /// <summary>
    /// A minimal
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false)]
    public sealed class JsonPropertyAttribute : Attribute
    {
        public string PropertyName { get; set; }
    
        public JsonPropertyAttribute(string propertyName)
        {
            PropertyName = propertyName;
        }
    }
}

namespace RavenTestConsole.RavenTests
{
    public class IndexesIgnoreNewtonsoftJsonPropertyAttributes : RavenTest
    {
        private class StudentDto
        {
            [Raven.Imports.Newtonsoft.Json.Sample.JsonProperty("EmailAddress")]
            public string Email { get; set; }
        
            [Raven.Imports.Newtonsoft.Json.JsonProperty("ZipCode")]
            public string Postcode { get; set; }
        }

        private class StudentDtos_ByEmailDomain : AbstractIndexCreationTask<StudentDto, StudentDtos_ByEmailDomain.Result>
        {
            public class Result
            {
                public string Email { get; set; }
                public string Postcode { get; set; }
            }

            public StudentDtos_ByEmailDomain()
            {
                Map = studentDtos => from studentDto in studentDtos
                                  select new Result
                                  {
                                      Email = studentDto.Email,
                                      Postcode = studentDto.Postcode
                                  };
            }
        }

        /// <summary>
        /// JsonProperty on Email is not Raven's version of the attribute, which means when objects
        /// of that type are serialized into Raven that change from Email to EmailAddress will be ignored.
        /// The serialization will respect the JsonProperty on the Postcode because it is Raven's version
        /// of the attribute. This test ensures that the creation of Maps in Indexes obey the same rule (i.e.
        /// they ignore the attribute on Email but obey the attribute on Postcode.
        /// </summary>
        [Fact]
        public void WillIgnoreAttribute()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.PrettifyGeneratedLinqExpressions = false;
                new StudentDtos_ByEmailDomain().Execute(store);

                var definition = store.DatabaseCommands.GetIndex(new StudentDtos_ByEmailDomain().IndexName);

                Assert.Equal(@"docs.StudentDtos.Select(studentDto => new {
    Email = studentDto.Email,
    Postcode = studentDto[""ZipCode""]
})", definition.Map);

                Assert.NotEqual(@"docs.StudentDtos.Select(studentDto => new {
    Email = studentDto[""EmailAddress""],
    Postcode = studentDto[""ZipCode""]
})", definition.Map);
            }
        }

        /// <summary>
        /// Checks that the index definitions actually compiles and run as expected on the server
        /// </summary>
        [Fact]
        public void WillGenerateIndexDefinitions()
        {
            using (var store = NewDocumentStore())
            {
                new StudentDtos_ByEmailDomain().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new StudentDto {Email = "a@b.c", Postcode = "30900"});
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var res =session.Query<StudentDto>().Single(x => x.Postcode.Equals("30900"));
                    Assert.Equal(res.Postcode,"30900");
                }
            }
        }
    }
}
