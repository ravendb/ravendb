using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Tests.Document;
using Raven.Database;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class OverwriteIndexLocally : LocalClientTest
    {
        [Fact]
        public void CanOverwriteIndex()
        {
            using(var store = NewDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Name }"
                                                }, overwrite:true);


                store.DatabaseCommands.PutIndex("test",
                                               new IndexDefinition
                                               {
                                                   Map = "from doc in docs select new { doc.Name }"
                                               }, overwrite: true);

                store.DatabaseCommands.PutIndex("test",
                                                new IndexDefinition
                                                {
                                                    Map = "from doc in docs select new { doc.Email }"
                                                }, overwrite: true);

                store.DatabaseCommands.PutIndex("test",
                                           new IndexDefinition
                                           {
                                               Map = "from doc in docs select new { doc.Email }"
                                           }, overwrite: true);
            }
        }
    }
}
