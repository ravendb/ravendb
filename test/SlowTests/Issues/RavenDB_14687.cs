using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch.Jint;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14687 : RavenTestBase
    {
        public RavenDB_14687(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task IndexSpecificSettingShouldBeRespected(Options options)
        {
            var initialStrictModeForScript = false;
            var initialMaxStepsForScript = 10;
            var initialMaxDurationForScript = new TimeSetting(20, TimeUnit.Milliseconds);
            options.ModifyDatabaseRecord +=record =>
            {
                //TODO: egor make this work & add test that create 2 indexes 1 jint & 1 v8 on same db
                //record.Settings[RavenConfiguration.GetKey(x => x.Indexing.JsStrictMode)] = initialStrictModeForScript.ToString();
                //record.Settings[RavenConfiguration.GetKey(x => x.Indexing.JsMaxSteps)] = initialMaxStepsForScript.ToString();
                //record.Settings[RavenConfiguration.GetKey(x => x.Indexing.JsMaxDuration)] = initialMaxDurationForScript.GetValue(TimeUnit.Milliseconds).ToString();
            }
            ;
            using (var store = GetDocumentStore(options))
            {
                var index = new MyJSIndex(options, null, null, null);
                index.Execute(store);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                //TODO: egor
                var indexInstance1 = (MapIndex)database.IndexStore.GetIndex(index.IndexName);
                var compiled1 = (AbstractJavaScriptIndex<JsHandleJint>)indexInstance1._compiled;

                Assert.Equal(initialStrictModeForScript, compiled1.EngineHandle.JsOptions.StrictMode);
                Assert.Equal(initialMaxStepsForScript, compiled1.EngineHandle.JsOptions.MaxSteps);
                Assert.Equal(initialMaxDurationForScript.GetValue(TimeUnit.Milliseconds), compiled1.EngineHandle.JsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds));

                const bool strictModeForScript = true;
                const int maxStepsForScript = 1001;
                var maxDurationForScript = new TimeSetting(101, TimeUnit.Milliseconds);
                index = new MyJSIndex(options, strictModeForScript, maxStepsForScript, maxDurationForScript);
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                var indexInstance2 = (MapIndex)database.IndexStore.GetIndex(index.IndexName);
                var compiled2 = (AbstractJavaScriptIndex<JsHandleJint>)indexInstance2._compiled;

                Assert.NotEqual(indexInstance1, indexInstance2);
                Assert.NotEqual(compiled1, compiled2);

                Assert.Equal(strictModeForScript, compiled2.EngineHandle.JsOptions.StrictMode);
                Assert.Equal(maxStepsForScript, compiled2.EngineHandle.JsOptions.MaxSteps);
                Assert.Equal(maxDurationForScript.GetValue(TimeUnit.Milliseconds), compiled2.EngineHandle.JsOptions.MaxDuration.GetValue(TimeUnit.Milliseconds));

                using (var session = store.OpenSession())
                {
                    session.Store(new Company());

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);
            }
        }

        private class MyJSIndex : AbstractJavaScriptIndexCreationTask
        {
            public MyJSIndex(Options options, bool? strictModeForScript, int? maxStepsForScript, TimeSetting? maxDurationForScript)
            {
                var optionalChaining = options.JavascriptEngineMode.ToString() switch
                {
                    "Jint" => "",
                    "V8" => "?",
                    _ => throw new NotSupportedException($"Not supported jsEngineType '{options.JavascriptEngineMode.ToString()}'.")
                };

                var mapCode = @"
map('Companies', (company) => {
/*JINT_START*/
//})
/*JINT_END*/
    var x = [];
    for (var i = 0; i < 50; i++) {
        x.push(i);
    }
    if (company.Address{optionalChaining}.Country === 'USA') {
        return {
            Name: company.Name,
            Phone: company.Phone,
            City: company.Address.City
        };
    }
})";

                mapCode = mapCode.Replace("{optionalChaining}", optionalChaining);
                
                Maps = new HashSet<string>()
                {
                    mapCode
                };
                //TODO: egor
                //if (strictModeForScript.HasValue)
                //    Configuration[RavenConfiguration.GetKey(x => x.Indexing.JsStrictMode)] = strictModeForScript.ToString();
                if (maxStepsForScript.HasValue)
                    Configuration[RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript)] = maxStepsForScript.ToString();
                //if (maxDurationForScript.HasValue)
                //    Configuration[RavenConfiguration.GetKey(x => x.Indexing.JsMaxDuration)] = maxDurationForScript?.GetValue(TimeUnit.Milliseconds).ToString();
            }
        }
    }
}
