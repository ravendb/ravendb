extern alias database;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Indexing;
using Raven.Bundles.MoreLikeThis;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.MoreLikeThis;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Bundles.Tests.MoreLikeThis
{
    public class MoreLikeThisTests : IDisposable
    {
        private readonly DocumentStore documentStore;
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;

		public MoreLikeThisTests()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(MoreLikeThisTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory("Data");
			ravenDbServer = new RavenDbServer(
				new database::Raven.Database.Config.RavenConfiguration
				{
					Port = 8080,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					DataDirectory = path,
                    Catalog = { Catalogs = { new AssemblyCatalog(typeof(MoreLikeThisResponder).Assembly) } },
				});

			documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			};
			documentStore.Initialize();
		}

        static public void WaitForUserToContinueTheTest(DocumentStore documentStore)
        {
            if (Debugger.IsAttached == false)
                return;

            documentStore.DatabaseCommands.Put("Pls Delete Me", null,

                                               RavenJObject.FromObject(new { StackTrace = new StackTrace(true) }),
                                               new RavenJObject());

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Get("Pls Delete Me") != null);
            
        }

        #region IDisposable Members

        public void Dispose()
        {
            documentStore.Dispose();
            ravenDbServer.Dispose();
            database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
        }

        #endregion

        [Fact]
        public void Basic_Test_Returning_String()
        {
            //InsertData();

            const string key = "datas/1";

            using (var session = documentStore.OpenSession())
            {
                new DataBodyIndex().Execute(documentStore);

                var list = new List<Data>
                               {
                                   new Data {Body = "This is a test. Isn't it great?"},
                                   new Data {Body = "I have a test tomorrow. I hate having a test"},
                                   new Data {Body = "Cake is great."},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"}
                               };
                list.ForEach(session.Store);

                session.SaveChanges();

                //Ensure non stale index
                var testObj = session.Query<Data, DataBodyIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == key).SingleOrDefault();
            }

            

            using (var session = documentStore.OpenSession())
            {
                var testObj = session.Query<Data, DataBodyIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == key).SingleOrDefault();
                Assert.NotNull(testObj);

                //WaitForUserToContinueTheTest(documentStore);

                var str = session.Advanced.MoreLikeThis("DataBodyIndex", key, "Body");

                Assert.False(String.IsNullOrEmpty(str));
            }
        }

        private void InsertData()
        {
            using (var session = documentStore.OpenSession())
            {
                new DataBodyIndex().Execute(documentStore);

                var list = new List<Data>
                               {
                                   new Data {Body = "This is a test. Isn't it great?"},
                                   new Data {Body = "I have a test tomorrow. I hate having a test"},
                                   new Data {Body = "Cake is great."},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"},
                                   new Data {Body = "test"}
                               };
                //list.ForEach(session.Store);

                foreach (var data in list)
                {
                    session.Store(data);
                }

                session.SaveChanges();

                //Ensure non stale index
                var testObj = session.Query<Data, DataBodyIndex>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Id == list[0].Id).SingleOrDefault();
            }
        }

        #region Data Classes

        public class Data
        {
            public string Id { get; set; }
            public string Body { get; set; }
        }

        #endregion

        #region "Indexes"

        public class DataBodyIndex : AbstractIndexCreationTask<Data>
        {
            public DataBodyIndex()
            {
                Map = docs => from doc in docs
                              select new {doc.Id, doc.Body};

                Analyzers = new Dictionary<Expression<Func<Data, object>>, string>
                                {
                                    {
                                        x => x.Body,
                                        typeof (StandardAnalyzer).FullName
                                    }
                                };

                Stores = new Dictionary<Expression<Func<Data, object>>, FieldStorage>
                             {
                                 {
                                     x => x.Body, FieldStorage.Yes
                                 }
                             };

            }
        }

        #endregion

    }
}
