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
                        Numbers = new List<int> { 1, 2, 3 },
                        Tries = new List<Message.Try> { new() { ResultMessage = "Received" } },
                        Data = new List<Message.Info>
                        {
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 10, ProductName = "Screen", InternalItem = new Message.Info.Item { TotalPrice = 10 }},
                                    new() { TotalPrice = 20, ProductName = "TV", InternalItem = new Message.Info.Item { TotalPrice = 20 }},
                                    new() { TotalPrice = 30, ProductName = "Screen", InternalItem = new Message.Info.Item { TotalPrice = 30 }}
                                },
                            },
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 10, ProductName = "TV", InternalItem = new Message.Info.Item { TotalPrice = 10 }},
                                    new() { TotalPrice = 20, ProductName = "Screen", InternalItem = new Message.Info.Item { TotalPrice = 20 }},
                                    new() { TotalPrice = 30, ProductName = "Table", InternalItem = new Message.Info.Item { TotalPrice = 30 }}
                                },
                            }
                        }
                    },
                    new()
                    {
                        Name = "Initial",
                        Numbers = new List<int> { 1, 2, 3 },
                        Tries = new List<Message.Try> { new() { ResultMessage = "Received" } },
                        Data = new List<Message.Info>
                        {
                            new()
                            {
                                Items = new List<Message.Info.Item>
                                {
                                    new() { TotalPrice = 1, ProductName = "TV", InternalItem = new Message.Info.Item { TotalPrice = 1 }},
                                    new() { TotalPrice = 2, ProductName = "Table", InternalItem = new Message.Info.Item { TotalPrice = 2 }},
                                    new() { TotalPrice = 3, ProductName = "Laptop", InternalItem = new Message.Info.Item { TotalPrice = 3 }}
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
                              "select Name, Tries[].ResultMessage as ResultMessage, sum(Data.Items[].TotalPrice) as Total";

                    var results = session.Advanced.RawQuery<ResultDifferentPath>(rql).ToList();
                    Assert.Equal(0, results.Count); // expected - wrong rql will produce 0 results

                    rql = "from 'Messages' " +
                          "group by Name, Tries[].ResultMessage " +
                          "select Name, Tries[].ResultMessage as ResultMessage, sum(Data[].Items[].TotalPrice) as Total";

                    results = session.Advanced.RawQuery<ResultDifferentPath>(rql).ToList();
                    Assert.Equal(1, results.Count);
                    Assert.Equal("Initial", results[0].Name);
                    Assert.Equal("Received", results[0].ResultMessage);
                    Assert.Equal(126, results[0].Total);

                    rql = "from 'Messages' " +
                          "group by Name, Data[].Items[].ProductName " +
                          "select Name, Data[].Items[].ProductName as ProductName, sum(Data[].Items[].TotalPrice) as Total";

                    var resultsSamePath = session.Advanced.RawQuery<ResultSamePath>(rql).ToList();
                    Assert.Equal(4, resultsSamePath.Count);

                    Assert.True(resultsSamePath.Select(x => x.Name).All(x => x == "Initial"));

                    var dictionary = resultsSamePath.ToDictionary(x => x.ProductName, x => x.Total);

                    Assert.Equal(60, dictionary["Screen"]);
                    Assert.Equal(31, dictionary["TV"]);
                    Assert.Equal(32, dictionary["Table"]);
                    Assert.Equal(3, dictionary["Laptop"]);

                    rql = "from 'Messages' " +
                          "group by Name, Data[].Items[].ProductName " +
                          "select Name, Data[].Items[].ProductName as ProductName, sum(Data[].Items[].InternalItem.TotalPrice) as Total";

                    resultsSamePath = session.Advanced.RawQuery<ResultSamePath>(rql).ToList();
                    Assert.Equal(4, resultsSamePath.Count);

                    Assert.True(resultsSamePath.Select(x => x.Name).All(x => x == "Initial"));

                    dictionary = resultsSamePath.ToDictionary(x => x.ProductName, x => x.Total);

                    Assert.Equal(60, dictionary["Screen"]);
                    Assert.Equal(31, dictionary["TV"]);
                    Assert.Equal(32, dictionary["Table"]);
                    Assert.Equal(3, dictionary["Laptop"]);

                    rql = "from 'Messages' " +
                          "group by Name, Numbers[] " +
                          "select Name, Numbers[] as OriginalNumber, sum(Numbers) as Total";

                    var numbersResult = session.Advanced.RawQuery<ResultSamePathNumbers>(rql).ToList();
                    Assert.Equal(3, numbersResult.Count);
                    Assert.True(numbersResult.Select(x => x.Name).All(x => x == "Initial"));

                    Assert.Equal(1, numbersResult[0].OriginalNumber);
                    Assert.Equal(2, numbersResult[0].Total);
                    Assert.Equal(2, numbersResult[1].OriginalNumber);
                    Assert.Equal(4, numbersResult[1].Total);
                    Assert.Equal(3, numbersResult[2].OriginalNumber);
                    Assert.Equal(6, numbersResult[2].Total);
                }
            }
        }


        private class Message
        {
            public string Name { get; set; }

            public List<int> Numbers { get; set; }

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

                    public Item InternalItem { get; set; }
                }
            }
        }

        private class ResultDifferentPath
        {
            public string Name { get; set; }

            public double Total { get; set; }

            public string ResultMessage { get; set; }
        }

        private class ResultSamePath
        {
            public string Name { get; set; }

            public double Total { get; set; }

            public string ProductName { get; set; }
        }

        private class ResultSamePathNumbers
        {
            public string Name { get; set; }

            public double Total { get; set; }

            public int OriginalNumber { get; set; }
        }
    }
}
