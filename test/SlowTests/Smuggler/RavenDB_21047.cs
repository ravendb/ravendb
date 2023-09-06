using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class RavenDB_21047 : RavenTestBase
    {
        private const string _docId = "TestObjs/0";
        private const string _changeVector = "C:10323059-46A/cZk8/UuwYdG5Dji0Sg, B:455420525-C3U1veikQEKrM2RUNtZQ5A, B:641180022-DxzjyxXS1EqgSEO3J5hgpw, " +
                                             "B:4797716-K8YhRiUCo0GzECyk/JJeKw, A:643125941-MJQwORy0mkes5E53+IAkrg, A:11784715-Mgugu/MVqEOgJNafGyKIfg, " +
                                             "B:3985724-QRjh5tTLokWSvDJDbF6JeQ, C:15033473-S1HWITSRQkSzFIy40y9ayg, A:8031522-UjG7submxUSZV+UH3Dx3+g, " +
                                             "C:456275656-VszrCLgjjE+jfH1HCyLRSA, B:15060618-ekC7E4AuOU2O8fgRey3iJA, B:10321238-ez7yDfG04k+VpltCkY5frw, " +
                                             "A:15575366-kd+eerHmN0ulK2L+gCPIeA, C:8198351-l1IVYfwRNUWjea1mFGMYhg, A:447847892-pX0Cwmvv50WIRxf45m2MtQ, " +
                                             "C:639094708-sb+TC4f3XEGcC7vnkjCTrA, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, " +
                                             "A:2568-OSKWIRBEDEGoAxbEIiFJeQ, A:17614-jxcHZAmE70Kb2y3I+eaWdw, A:29931-+oKB0eox0kKFRmpZhHdpYA, " +
                                             "A:58113-48XOT93s0keG852a0alBCA, A:80057-8MXXSiK/j02SPt6WvMBp3g, A:34139-J0psmFwcEk+f2D6rB5uSKg, " +
                                             "A:53822-RCHmPvRlA0qCqyNWNH7Ufg, A:51048-RL5qTmLhV0SwOwKPeOgf4A, A:105702-b8TkJER0y0mlX3cdUnPJ7A, " +
                                             "A:86566-hJl/w96QZ0S1Nrnj73I8kA, A:272221-lF+69deD8UCyPIClNBHGzA, A:53182-p3oH6z9j/0WoL33F0GT6dw, " +
                                             "A:65536-yAs2j2d7EUKMYy7+D+oBEw, B:67152-BgN7ypHCN0eZLbWQW4pSbA, B:29926-uAcwGmeZFECexya2bfY6MA, " +
                                             "B:54928-z0A6qAIgbkeiDwvkcMmsLQ, C:65537-3N4Tf0UvQUSG+Zg5q8f41A, C:58113-HdKAr6tiA0OhTCazgYuK8A, " +
                                             "C:54931-agGw7kyp/EmrroDbznOceQ, C:53822-fa7cPVbGKEWWXcPFlgCy0g, C:50774-lZTIFvUSQEiC7pboTgiYxg, " +
                                             "C:51048-qnNDTHlOPUWL/4484fXkz, C:53182-ts+A2CssQkiaYR3vSyIrlQ, D:54929-/15CzUoPak+RM2pwW2arnw, " +
                                             "D:54929-/15CzUoPak+RM2pwW4arnw, B:54929-/15CzUoPak+RM2pwW2arnO, B:54955-/15C4joPak+RM2pwW2arnO, " +
                                             "B:64929-/1589UoPak+RM2pwW2arnO, A:74929-/7jCzUoPak+RM2pwW2arnO, C:43929-/15CzUoPiu+RM2pwW2arnO";

        public RavenDB_21047(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Smuggler | RavenTestCategory.Revisions | RavenTestCategory.BackupExportImport)]
        public async Task ShouldThrowWhenRevisionIdExceedsTheMaximumIdLength()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { MinimumRevisionsToKeep = 1 } };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

            var database = await GetDatabase(store.Database);

            var exception = Assert.Throws<ArgumentException>(() =>
            {
                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        var value = new DynamicJsonValue
                        {
                            [Constants.Documents.Metadata.Key] = new DynamicJsonValue { [Constants.Documents.Metadata.Collection] = "TestObjs" }
                        };
                        using var doc = context.ReadObject(value, _docId, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                        database.DocumentsStorage.Put(context, _docId, null, doc, changeVector: _changeVector);
                        tx.Commit();
                    }
                }
            });

            Assert.Contains("Revision change vector cannot exceed 1536 bytes", exception.Message);
        }
    }
}
