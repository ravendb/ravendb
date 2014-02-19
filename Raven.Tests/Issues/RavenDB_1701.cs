// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1701.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Abstractions.Data;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1701 : RavenTest
    {
        private class OpCounter
        {
            public int Index { get; set; }
            public int Deletes { get; set; }
        }

        [Fact]
        public void CanLoadPutDocumentsMultipleTimesInPatcher()
        {

            using (var store = NewDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new OpCounter(), "opCounter");
                    s.SaveChanges();
                }
                var patcher =
                    new ScriptedIndexResultsIndexTrigger.Batcher.ScriptedIndexResultsJsonPatcher(
                        store.DocumentDatabase, new HashSet<string> { "dogs" });


                patcher.Apply(new RavenJObject(), new ScriptedPatchRequest
                {
                    Script =
@"var opCounterId = 'opCounter';
var opCounter = LoadDocument(opCounterId) || {};
opCounter.Index++;
PutDocument(opCounterId, opCounter);
opCounter = LoadDocument(opCounterId)
opCounter.Deletes++;
PutDocument(opCounterId, opCounter);
"
                });

                store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
                {
                    foreach (var operation in patcher.GetOperations())
                    {
                        var key = operation.Key;
                        var document = operation.Value;
                        var isPut = document != null;

                        if (isPut)
                            store.DocumentDatabase.Put(key, document.Etag, document.DataAsJson, document.Metadata, null);
                        else
                            store.DocumentDatabase.Delete(key, null, null);
                    }
                });

                using (var s = store.OpenSession())
                {
                    var opCounter = s.Load<OpCounter>("opCounter");
                    Assert.Equal(1, opCounter.Deletes);
                    Assert.Equal(1, opCounter.Index);
                }
            }
        } 
    }
}