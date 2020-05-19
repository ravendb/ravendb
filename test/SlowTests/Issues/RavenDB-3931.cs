// -----------------------------------------------------------------------
//  <copyright file="RavenDB-3931.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.IO;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3931 : NoDisposalNeeded
    {
        public RavenDB_3931(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public IEnumerable<string> Foo
            {
                get { yield return "foo"; yield return "bar"; }
            }
        }

        [Fact]
        public void CanSerializeYieldGetterMethods()
        {
            // If this test breaks, this is likely becaused we merged
            // a new version of JSON.Net, which can revert a modification that we made to the code
            // Look at the other changes that happened in this commit (see the git log for that)
            // And at any rate, the full explanation, including the full reasoning is here:
            // https://issues.hibernatingrhinos.com/issue/RavenDB-3931
            //
            // Don't try to fix this issue without reading the details, it is a single line fix, but it
            // takes time to get to the right reason

            var DocumentConventions = new DocumentConventions();
            var jsonSerializer = (JsonSerializer)DocumentConventions.Serialization.CreateSerializer();
            var stringWriter = new StringWriter();
            jsonSerializer.Serialize(stringWriter, new Item());
            var str = stringWriter.GetStringBuilder().ToString();
            jsonSerializer.Deserialize<Item>(new JsonTextReader(new StringReader(str)));
        }
    }
}
