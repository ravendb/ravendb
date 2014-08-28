using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2036 : RavenTest
    {

        public class Index__TestByName : AbstractIndexCreationTask
        {
			public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Map = @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""Raven-Entity-Name""] };",
                    Indexes =
                    {
                        {"Name", FieldIndexing.Analyzed}
                    }
                };
            }
        }
        //public class Dynamic___TestByName : AbstractIndexCreationTask
        //{
        //    public override IndexDefinition CreateIndexDefinition()
        //    {
        //        return new IndexDefinition
        //        {
        //            Map = @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""Raven-Entity-Name""] };",
        //            Indexes =
        //            {
        //                {"Name", FieldIndexing.Analyzed}
        //            }
        //        };
        //    }
        //}

     public class DynamicByNameIndex : AbstractIndexCreationTask
        {
		 public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Map = @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""Raven-Entity-Name""] };",
                    Indexes =
                    {
                        {"Name", FieldIndexing.Analyzed}
                    }
                };
            }
        }


     public class Dynamic : AbstractIndexCreationTask
     {
		 public override IndexDefinition CreateIndexDefinition()
         {
             return new IndexDefinition
             {
                 Map = @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""Raven-Entity-Name""] };",
                 Indexes =
                    {
                        {"Name", FieldIndexing.Analyzed}
                    }
             };
         }
     }
     public class Dynamic_ : AbstractIndexCreationTask
     {
		 public override IndexDefinition CreateIndexDefinition()
         {
             return new IndexDefinition
             {
                 Map = @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""Raven-Entity-Name""] };",
                 Indexes =
                    {
                        {"Name", FieldIndexing.Analyzed}
                    }
             };
         }
     }


        [Fact]
        public void CheckDynamicName()
        {
 
            using (var store = NewDocumentStore())
            {

                new DynamicByNameIndex().Execute(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var query = session
                        .Query<Tag, DynamicByNameIndex>()
                        .Customize(c => c.WaitForNonStaleResultsAsOfLastWrite())
                        .OrderBy(n => n.Name)
                        .ToList();
  
                    Assert.Equal(query.Count,0);
                }




            }


        }
        [Fact]
        public void DynamicUserDefinedIndexNameCreateFail()
        {

            using (var store = NewDocumentStore())
            {
                var ex = Assert.Throws<IndexCompilationException>(() => new Dynamic().Execute(store));
                Assert.True(ex.Message.Contains("Index name dynamic is reserved"));
               
            }
        }
        [Fact]
        public void Dynamic_IndexNameCreateFail()
        {

            using (var store = NewDocumentStore())
            {
                var ex = Assert.Throws<IndexCompilationException>(() => new Dynamic_().Execute(store));
                Assert.True(ex.Message.Contains("Index names starting with dynamic_ or dynamic/ are reserved."));

             }




       }


        
        [Fact]
        public void DynamicIndexOk()
        {

            using (var store = NewDocumentStore())
            {
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var query = session
                        .Query<Tag>()
                        .Customize(c => c.WaitForNonStaleResultsAsOfLastWrite())
                        .OrderBy(n => n.Name)
                        .ToList();

                    Assert.Equal(query.Count,0);

                }




            }


        }
        [Fact]
        public void DoubleUnderscoreIndexNameCreateFail()
        {

            using (var store = NewDocumentStore())
            {
                var ex = Assert.Throws<IndexCompilationException>(() => new Index__TestByName().Execute(store));
                Assert.True(ex.Message.Contains("Index names cannot contains // (double slashes)"));
            }


        }
    }


    public class Tag
    {
        public string Name { get; set; }
    }
}