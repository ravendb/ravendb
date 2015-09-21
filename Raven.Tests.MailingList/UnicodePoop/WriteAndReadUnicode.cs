// -----------------------------------------------------------------------
//  <copyright file="WriteAndReadUnicode.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using Newtonsoft.Json;
using Raven.Abstractions.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList.UnicodePoop
{
    public class WriteAndReadUnicode : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                using (var data = typeof (WriteAndReadUnicode).Assembly.GetManifestResourceStream(typeof (WriteAndReadUnicode).Namespace + ".activities-1958073.txt"))
                {
                    Assert.NotNull(data);

                    var ravenJObject = RavenJObject.Load(new RavenJsonTextReader(new StreamReader(data)));

                    store.DatabaseCommands.Put("test", null, ravenJObject, new RavenJObject());

                    Assert.NotNull(store.DatabaseCommands.Get("test"));
                }
            }
        }
    }
}