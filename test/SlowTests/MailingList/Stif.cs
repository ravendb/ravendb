using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Stif : RavenTestBase
    {
        [DataContract]
        private class MyDoc
        {
            [DataMember]
            public string Id { get; set; }
            [DataMember]
            public string Message { get; set; }
            [DataMember]
            public LogTypeEnum Type { get; set; }
        }

        [DataContract]
        private class Context
        {
            [DataMember]
            public string Key { get; set; }

            [DataMember]
            public string Value { get; set; }
        }

        [Flags]
        private enum LogTypeEnum
        {
            Usage = 1,
            Changes = 2,
            Error = 4,
            Performance = 8,
            Authentication = 16,
            ProductError = 32,
            ProductUsage = 64,
            NewsItem = 128,
            UserActivity = 256,
            All = 511
        }

        private class MyDocIndex : AbstractIndexCreationTask<MyDoc, MyDocIndex.MyDocResult>
        {
            public class MyDocResult
            {
                public string Message { get; set; }
                public bool IsChange { get; set; }
            }

            public MyDocIndex()
            {
                Map = docs => docs.Select(
                    lm => new MyDocResult()
                    {
                        Message = lm.Message,
                        IsChange = (lm.Type & LogTypeEnum.Changes) == LogTypeEnum.Changes
                    });

                Analyzers.Add(lm => lm.Message, "Lucene.Net.Analysis.Standard.StandardAnalyzer, Lucene.Net");
            }
        }

        [Fact]
        public void GetDummyDoc()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.SaveEnumsAsIntegers = true;
                }
            }))
            {
                new MyDocIndex().Execute(store);

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    documentSession.Store(new MyDoc { Id = new Guid(0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0).ToString(), Message = "some dummy message" });
                    documentSession.SaveChanges();
                }

                using (IDocumentSession documentSession = store.OpenSession())
                {
                    MyDoc docFetched = documentSession.Load<MyDoc>(new Guid(0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0).ToString()); //returns an object
                    Debug.WriteLine(string.Format("found {0}", docFetched.Id));

                    Assert.NotNull(docFetched);

                    List<MyDoc> docs = documentSession.Query<MyDoc>().Customize(x => x.WaitForNonStaleResults()).ToList(); //returns an empty list
                    Debug.WriteLine(string.Format("found {0} docs", docs.Count));

                    Assert.Equal(1, docs.Count);

                }
            }
        }
    }
}
