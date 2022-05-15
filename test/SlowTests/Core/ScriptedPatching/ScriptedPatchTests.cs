using System;
using System.Collections.Generic;
using System.Diagnostics;
using FastTests;
using FastTests.Server.JavaScript;
using Newtonsoft.Json;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Patching;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.ScriptedPatching
{
    public class ScriptedPatchTests : RavenTestBase
    {
        public ScriptedPatchTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void PatchingWithParametersShouldWork(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenSession())
            {
                session.Store(new Company
                {
                    Name = "The Wall"
                }, "companies/1");

                session.SaveChanges();
            }

            var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery()
            {
                Query = "from Companies update { this.Name = args.name }",
                QueryParameters = new Parameters()
                {
                    {"name", "Jon"}
                }
            }));

            operation.WaitForCompletion(TimeSpan.FromSeconds(15));

            using (var session = store.OpenSession())
            {
                var c = session.Load<Company>("companies/1");

                Assert.Equal("Jon", c.Name);
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void PatchingShouldThrowProperException(Options options)
        {
            var ttl = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(15);
            using var store = GetDocumentStore(options);
            using (var session = store.OpenSession())
            {
                session.Store(new Supplier
                {
                    ProcessRules = new List<ProcessRules>
                    {
                        new ProcessRules
                        {
                            BatchClass = "Foo",
                            Rules = new List<Rules>
                            {
                                new Rules
                                {
                                    Code = 9,
                                    PermittedDocumentAge = new PermittedDocumentAge
                                    {
                                        Age = 12,
                                        Unit = "Month"
                                    }
                                },
                                new Rules
                                {
                                    Code = 10,
                                    PermittedDocumentAge = new PermittedDocumentAge
                                    {
                                        Age = 12,
                                        Unit = "Month"
                                    }
                                }
                            }
                        },
                    }
                }, "foo/bar");

                session.SaveChanges();
            }

            var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery()
            {
                Query = @"from Suppliers 
Update
{
    for (var i = 0; i < this.ProcessRules.length; i++) 
    {
        var processRule = this.ProcessRules[i];
        var ruleFound = false;
    
        for (var j = 0; j < processRule.Rules.length; j++) 
        {
            var rule = processRule.Rules[j];
            if(rule.Code == 10)
            {
                rule.DecimalValue = 3;
                rule.PermittedDocumentAge.Unit = 'Months';
                rule.PermittedDocumentAge.Age = 3;
            }
        }
    }
}"
            }));
            operation.WaitForCompletion(ttl);

            using (var session = store.OpenSession())
            {
                session.Store(new Supplier
                {
                    ProcessRules = new List<ProcessRules>
                    {
                        new ProcessRules
                        {
                            BatchClass = "Foo",
                            Rules = new List<Rules>
                            {
                                new Rules
                                {
                                    Code = 9,
                                    PermittedDocumentAge = new PermittedDocumentAge
                                    {
                                        Age = 12,
                                        Unit = "Month"
                                    }
                                },
                                new Rules
                                {
                                    Code = 10,
                                }
                            }
                        },
                    }
                }, "foo/bar");

                session.SaveChanges();
            }

            operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery()
            {
                Query = @"from Suppliers 
Update
{
    for (var i = 0; i < this.ProcessRules.length; i++) 
    {
        var processRule = this.ProcessRules[i];
        var ruleFound = false;
    
        for (var j = 0; j < processRule.Rules.length; j++) 
        {
            var rule = processRule.Rules[j];
            if(rule.Code == 10)
            {
                rule.DecimalValue = 3;
                rule.PermittedDocumentAge.Unit = 'Months';
                rule.PermittedDocumentAge.Age = 3;
            }
        }
    }
}"
            }));
            var e = Assert.Throws<JavaScriptException>(() => operation.WaitForCompletion(ttl));
            Assert.Contains("Unit", e.Message);
            Assert.Contains("defined", e.Message);
        }

        private class PermittedDocumentAge
        {
            public int Age { get; set; }
            public string Unit { get; set; }
        }

        private class Rules
        {
            public int Code { get; set; }
            public int DecimalValue { get; set; }
            [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
            public PermittedDocumentAge PermittedDocumentAge { get; set; }
        }

        private class ProcessRules
        {
            public string BatchClass { get; set; }
            public List<Rules> Rules { get; set; }
        }

        private class Supplier
        {
            public List<ProcessRules> ProcessRules { get; set; }
        }
    }
}
