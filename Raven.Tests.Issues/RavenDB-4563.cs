using System;
using System.Threading;
using Raven.Client.Document;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4563 : RavenTestBase
    {
        [Fact]
        public void bulk_insert_throws_when_server_is_down()
        {
            using (var server = GetNewServer(port: 8079))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8079",
                DefaultDatabase = "test"
            }.Initialize())
            {
                Exception exp = null;
                for (var run = 0; run < 4; run++)
                {
                    try
                    {
                        using (var bulkInsert = store.BulkInsert())
                        {
                            for (var j = 0; j < 10000; j++)
                            {
                                bulkInsert.Store(new Sample());

                                if (j == 5000 && run == 1)
                                {
                                    server.Dispose();
                                    Thread.Sleep(1000);
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        exp = e;
                    }
                    finally
                    {
                        switch (run)
                        {
                            case 0:
                                Assert.Equal(null, exp);
                                break;
                            case 1:
                                Assert.NotNull(exp.Message);
                                break;
                            case 2:
                                Assert.Equal("Could not get token for bulk insert", exp.Message);
                                Assert.Equal("An error occurred while sending the request.", exp.InnerException.Message);
                                Assert.Equal("Unable to connect to the remote server", exp.InnerException.InnerException.Message);
                                break;
                            case 3:
                                Assert.Equal("Could not get token for bulk insert", exp.Message);
                                Assert.Equal("An error occurred while sending the request.", exp.InnerException.Message);
                                Assert.Equal("Unable to connect to the remote server", exp.InnerException.InnerException.Message);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }
            }
        }

        public class Sample
        {
            public string Id { get; set; }
        }
    }
}