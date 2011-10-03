using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Linq;
using Raven.Json.Linq;

namespace RavenView.Views
{
    public class TestClassView : AbstractViewGenerator
    {
        public TestClassView()
        {

            ForEntityNames.Add("TestClass");
            MapDefinitions.Add(MapToPaths);
            ReduceDefinition = Reduce;
            GroupByExtraction = doc => doc.UserId;

            AddField("UserId");
            AddField("Name");
            AddField("Email");

            Indexes.Add("UserId", FieldIndexing.NotAnalyzed);

            
        }


        private IEnumerable<dynamic> Reduce(IEnumerable<dynamic> source)
        {
            foreach (var o in source)
            {
                //Console.WriteLine("{0},{1}",o.__document_id, o.UserId);
                yield return new
                                 {
                                     o.__document_id,
                                     o.UserId,
                                     o.Name,
                                     o.Email
                                 };
            }
        }

        private IEnumerable<dynamic> MapToPaths(IEnumerable<dynamic> source)
        {
            foreach (var o in source)
            {
                var testClass = FromRaven(o);
                foreach (var item in testClass.Items)
                {
                    yield return new
                                     {
                                         __document_id = o.Id,
                                         UserId = item.Id,
                                         item.Name,
                                         item.Email
                                     };
                }
            }    
            yield break;
        }

       
        TestClass FromRaven(dynamic o) 
        {
            var jobject = (RavenJObject)o.Inner;
            var item = ((TestClass)jobject.Deserialize(typeof(TestClass),  Conventions.Document ));

            if (item == null)
                throw new ApplicationException("Deserialisation failed");

            return item;
        }
    }

 

    public static class Conventions
    {
        public static readonly DocumentConvention Document = new DocumentConvention
        {
            FindTypeTagName = t => t.GetType() == typeof(TestClass) ? "testclass" : null,
            MaxNumberOfRequestsPerSession = 3000,
            DocumentKeyGenerator = doc =>
            {
                if (doc is TestClass)
                    return ((TestClass)doc).Id;

                return null;
            }
        };
    }


}
