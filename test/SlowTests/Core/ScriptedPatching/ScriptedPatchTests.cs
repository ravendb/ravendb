﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using FastTests;
using Newtonsoft.Json;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
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

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Patching)]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(true, DatabaseMode = RavenDatabaseMode.Single)]
        public void CanAllowStringCompilation(Options options, bool allowStringCompilation)
        {
            options.ModifyDatabaseRecord += record => record.Settings[RavenConfiguration.GetKey(x => x.Patching.AllowStringCompilation)] = allowStringCompilation.ToString();

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
                Query = @"from Companies update { 
                            const script = 'return ""Hello World"";';
                            const dynoFunc = new Function(""doc"", script);
                            this.Name = dynoFunc(company);
                        }",
            }));

            if (allowStringCompilation)
            {
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var c = session.Load<Company>("companies/1");

                    Assert.Equal("Hello World", c.Name);
                }
            }
            else
            {
                var error = Assert.Throws<JavaScriptException>(() =>
                {
                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                });
                Assert.Contains("String compilation has been disabled in engine options. You can configure it by modifying the configuration option: 'Patching.AllowStringCompilation'",
                    error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void PatchingShouldThrowProperException()
        {
            var ttl = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(15);
            using var store = GetDocumentStore();
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
            Assert.Contains("Unit is not defined", e.Message);
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

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanUseUuidFunction(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var commands = store.Commands())
            {
                commands.Put("companies/1", null, new Company
                {
                    Name = "Test Company"
                }, null);

                // Test that uuid() generates a valid UUID
                store.Operations.Send(new PatchOperation("companies/1", null,
                    new PatchRequest
                    {
                        Script = @"this.Uuid = uuid();"
                    }));

                dynamic result = commands.Get("companies/1");
                Assert.NotNull(result.Uuid);
                
                // Verify it's a valid GUID format (36 characters with dashes)
                string uuid = result.Uuid.ToString();
                Assert.True(Guid.TryParse(uuid, out _), $"uuid() should generate a valid UUID, but got: {uuid}");
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void UuidFunctionGeneratesUniqueValues(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var commands = store.Commands())
            {
                commands.Put("companies/1", null, new Company
                {
                    Name = "Test Company"
                }, null);

                // Generate multiple UUIDs and ensure they are unique
                store.Operations.Send(new PatchOperation("companies/1", null,
                    new PatchRequest
                    {
                        Script = @"this.Uuid1 = uuid(); this.Uuid2 = uuid(); this.Uuid3 = uuid();"
                    }));

                dynamic result = commands.Get("companies/1");
                Assert.NotNull(result.Uuid1);
                Assert.NotNull(result.Uuid2);
                Assert.NotNull(result.Uuid3);
                
                string uuid1 = result.Uuid1.ToString();
                string uuid2 = result.Uuid2.ToString();
                string uuid3 = result.Uuid3.ToString();
                
                // Verify all three are different
                Assert.NotEqual(uuid1, uuid2);
                Assert.NotEqual(uuid2, uuid3);
                Assert.NotEqual(uuid1, uuid3);
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void UuidFunctionShouldThrowOnParameters(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var commands = store.Commands())
            {
                commands.Put("companies/1", null, new Company
                {
                    Name = "Test Company"
                }, null);

                // Test that uuid() throws when called with parameters
                var exception = Assert.Throws<JavaScriptException>(() =>
                {
                    store.Operations.Send(new PatchOperation("companies/1", null,
                        new PatchRequest
                        {
                            Script = @"this.Uuid = uuid('invalid');"
                        }));
                });

                Assert.Contains("must be called without arguments", exception.Message);
            }
        }
    }
}
