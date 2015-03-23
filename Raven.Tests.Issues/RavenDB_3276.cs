using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3276 : RavenTestBase
	{
	    [Fact]
	    public void Direct_request_with_empty_json_property_should_result_in_422_http_code()
	    {
			//http://[DB url]/databases/[DB name]/bulk_docs
		    var httpRequestFactory = new HttpJsonRequestFactory(1);
			const string body = "[{\"Key\":\"TestEntities/1\",\"Method\":\"PUT\",\"Document\":{\"Items\":{\"\":\"value for empty string\"}},\"Metadata\":{\"Raven-Entity-Name\":\"TestEntities\",\"Raven-Clr-Type\":\"Raven.Tests.Issues.RavenDB_3276+TestEntity, Raven.Tests.Issues\"},\"AdditionalData\":null,\"Etag\":\"00000000-0000-0000-0000-000000000000\"}]";

		    using (var store = NewRemoteDocumentStore())
		    {
			    var url = String.Format("{0}/databases/{1}/bulk_docs", store.Url, store.DefaultDatabase);
			    var request = httpRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, url, "POST", new OperationCredentials(null, CredentialCache.DefaultNetworkCredentials), new DocumentConvention()));

				var exception = Assert.Throws<AggregateException>(() => request.WriteAsync(body).Wait());
								
			    var realException = exception.InnerException as ErrorResponseException;
				Assert.Equal(422,(int)realException.StatusCode); 
		    }
	    }

		// 
	    [Fact]
	    public async Task Dictionary_with_empty_string_as_key_should_fail_bulk_insert_request()
	    {
		    var jsonRequestFactory = new HttpJsonRequestFactory(25);
		    using (var store = NewRemoteDocumentStore())
		    {
			    var url = String.Format("{0}/databases/{1}", store.Url, store.DefaultDatabase);
			    using (ConnectionOptions.Expect100Continue(url))
			    {
				    var operationUrl = "/bulkInsert?&operationId=" + Guid.NewGuid();
				    var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(null, url + operationUrl, "POST", new OperationCredentials(String.Empty, CredentialCache.DefaultNetworkCredentials), new DocumentConvention());
					var request = jsonRequestFactory.CreateHttpJsonRequest(createHttpJsonRequestParams);
					
					var response = await request.ExecuteRawRequestAsync((requestStream, tcs) => 
				    {
					    using (var bufferedStream = new MemoryStream())
					    {
						    long bytesWritten;
						    WriteToBuffer(bufferedStream, out bytesWritten);

							var requestBinaryWriter = new BinaryWriter(requestStream);
							requestBinaryWriter.Write((int)bufferedStream.Position);

						    bufferedStream.WriteTo(requestStream);
						    requestStream.Flush();
						    tcs.TrySetResult(null);
					    }
				    });

					Assert.Equal(422, (int)response.StatusCode);
			    }
		    }
	    }

		private void WriteToBuffer(Stream bufferedStream, out long bytesWritten)
		{
			using (var gzip = new GZipStream(bufferedStream, CompressionMode.Compress, leaveOpen: true))
			using (var stream = new CountingStream(gzip))
			{
				var binaryWriter = new BinaryWriter(stream);
				binaryWriter.Write(1);
				var bsonWriter = new BsonWriter(binaryWriter)
				{
					DateTimeKindHandling = DateTimeKind.Unspecified
				};

				bsonWriter.WriteStartObject();

				bsonWriter.WritePropertyName(String.Empty);
				bsonWriter.WriteValue("ABCDEFG");

				bsonWriter.WriteEndObject();

				bsonWriter.Flush();
				binaryWriter.Flush();
				stream.Flush();
				bytesWritten = stream.NumberOfWrittenBytes;
			}
		}

		[Fact]
		public void Dictionary_with_empty_string_as_key_should_fail_storing_in_db()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(
						new TestEntity
						{
							Items = new Dictionary<string, string>
							{
								{"", "value for empty string"}
							}
						});

					Assert.Throws<InvalidDataException>(() => session.SaveChanges());
				}
			}
		}
	
		[Fact]
		public void Dictionary_with_empty_string_as_key_should_fail_bulk_insert()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var bulkInsert = store.BulkInsert())
				{
					var e = Assert.Throws<InvalidDataException>(() => 
					bulkInsert.Store(new TestEntity
					{
						Items = new Dictionary<string, string>
						{
							{"", "value for empty string"}
						}
					}));
				}
			}
		}


		class TestEntity
		{
			public string Id { get; set; }
			public Dictionary<string, string> Items { get; set; }
		}
	}
}
