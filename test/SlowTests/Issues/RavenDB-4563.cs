using System;
using System.Threading;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4563 : RavenTestBase
    {
        [Fact]
        public void bulk_insert_throws_when_server_is_down()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
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

                                if (j == 500 && run == 1)
                                {
                                    Server.Dispose();
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
                                Assert.Equal("Could not read from server, it status is faulted",exp.Message);
                                break;
                            case 2:
                                Assert.Equal("Unable to connect to the remote server", exp.Message);
                                break;
                            case 3:
                                Assert.Equal("Unable to connect to the remote server", exp.Message);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }
                }
            }
        }

        private class Sample
        {
            public string Id { get; set; }
        }
    }
}