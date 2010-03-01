using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Rhino.DivanDB.Server;
using Xunit;

namespace Rhino.DivanDB.Client.Tests
{
    public class DocumentStoreServerTests : BaseTest
    {
        [Fact]
        public void Should_insert_into_db_and_set_id()
        {
            DivanServer.EnsureCanListenToWhenInNonAdminContext(8080);
            using (var server = new DivanServer(DbName, 8080))
            {
                var documentStore = new DocumentStore("localhost", 8080);
                var session = documentStore.OpenSession();
                var entity = new Company { Name = "Pap" };
                session.Store(entity);

                Assert.Equal(Guid.Empty.ToString(), entity.Id);
//                var request = WebRequest.Create("http://localhost:8080" + "/docs");
//                request.Method = "POST";
//
//                
//                var json = JsonConvert.SerializeObject(entity);
//                byte[] byteArray = Encoding.UTF8.GetBytes(json);
//                request.ContentType = "application/json";
//
//                var dataStream = request.GetRequestStream();
//                // Write the data to the request stream.
//                dataStream.Write(byteArray, 0, byteArray.Length);
//
//                // Close the Stream object.
//                dataStream.Close();
//
//                var response = request.GetResponse();
//                var responseString = response.GetResponseStream();
//                Console.WriteLine(((HttpWebResponse)response).StatusDescription);
//                var reader = new StreamReader(responseString);
//                var returned = reader.ReadToEnd();
            }
        }
    }
}