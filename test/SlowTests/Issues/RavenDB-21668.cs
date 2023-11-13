using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21668 : RavenTestBase
    {
        public RavenDB_21668(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_Query_Map_Reduce_Index()
        {
            using (var store = GetDocumentStore())
            {
                var messages = new List<Message>
                {
                    new()
                    {
                        Name = "Initial",
                        Tries = new List<Message.Try> { new() { ResultMessage = "Received" } },
                        Data = new List<Message.Info>
                        {
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 10, ProductName = "Screen" },
                                    new() { TotalPrice = 20, ProductName = "TV" },
                                    new() { TotalPrice = 30, ProductName = "Screen" }
                                },
                            },
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 10, ProductName = "TV" },
                                    new() { TotalPrice = 20, ProductName = "Screen" },
                                    new() { TotalPrice = 30, ProductName = "Table" }
                                },
                            }
                        }
                    },
                    new()
                    {
                        Name = "Initial",
                        Tries = new List<Message.Try> { new() { ResultMessage = "Received" } },
                        Data = new List<Message.Info>
                        {
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 1, ProductName = "TV" },
                                    new() { TotalPrice = 2, ProductName = "Table" },
                                    new() { TotalPrice = 3, ProductName = "Laptop" }
                                }
                            }
                        }
                    }
                };

                using (var session = store.OpenSession())
                {
                    foreach (var message in messages)
                    {
                        session.Store(message);
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var rql = "from 'Messages' " +
                              "group by Name, Tries[].ResultMessage " +
                              "select sum(Data.Items[].TotalPrice) as Total";

                    var results = session.Advanced.RawQuery<ResultDifferentPath>(rql).ToList();
                    Assert.Equal(0, results.Count); // expected - wrong rql will produce 0 results

                    rql = "from 'Messages' " +
                          "group by Name, Tries[].ResultMessage " +
                          "select Name, sum(Data[].Items[].TotalPrice) as Total";

                    results = session.Advanced.RawQuery<ResultDifferentPath>(rql).ToList();
                    Assert.Equal(1, results.Count);
                    Assert.Equal("Initial", results[0].Name);
                    Assert.Equal(126, results[0].Total);

                    rql = "from 'Messages' " +
                          "group by Name, Data[].Items[].ProductName " +
                          "select Name, Data[].Items[].ProductName as ProductName, sum(Data[].Items[].TotalPrice) as Total";

                    var resultsSamePath = session.Advanced.RawQuery<ResultSamePath>(rql).ToList();
                    Assert.Equal(4, resultsSamePath.Count);

                    Assert.True(resultsSamePath.Select(x => x.Name).All(x => x == "Initial"));

                    Assert.Equal("Screen", resultsSamePath[0].ProductName);
                    Assert.Equal(60, resultsSamePath[0].Total);

                    Assert.Equal("TV", resultsSamePath[1].ProductName);
                    Assert.Equal(31, resultsSamePath[1].Total);

                    Assert.Equal("Table", resultsSamePath[2].ProductName);
                    Assert.Equal(32, resultsSamePath[2].Total);

                    Assert.Equal("Laptop", resultsSamePath[3].ProductName);
                    Assert.Equal(3, resultsSamePath[3].Total);
                }
            }
        }


        private class Message
        {
            public string Name { get; set; }

            public List<Info> Data { get; set; }

            public List<Try> Tries { get; set; }

            public class Try
            {
                public string ResultMessage { get; set; }
            }

            public class Info
            {
                public List<Item> Items { get; set; }

                public class Item
                {
                    public int TotalPrice { get; set; }

                    public string ProductName { get; set; }
                }
            }
        }

        private class ResultDifferentPath
        {
            public string Name { get; set; }

            public double Total { get; set; }
        }

        private class ResultSamePath : ResultDifferentPath
        {
            public string ProductName { get; set; }
        }

    }
}
