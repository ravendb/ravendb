using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Client.Connection;
using Raven.Client.Document;
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
					Assert.Throws<InvalidDataException>(() => 
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
