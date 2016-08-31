// -----------------------------------------------------------------------
//  <copyright file="PeterBalzli.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace SlowTests.MailingList
{
    public class PeterBalzli : RavenTestBase
    {
        protected override void ModifyStore(DocumentStore store)
        {
            store.Conventions.CustomizeJsonSerializer = serializers => serializers.Converters.Add(new CustomerNumberJsonConverter());
        }

        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Index());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item() { Id = "1" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session
                        .Query<Index.Result, Index>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ProjectFromIndexFieldsInto<Index.Result>()
                        .ToList();

                    Assert.Equal(1, items.Count);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }
            public CustomerNumber Number { get; set; }
        }

        private class Customer1
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class CustomerNumber
        {
            public string Number { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Item, Index.Result>
        {

            public class Result
            {
                public string Id { get; set; }
                public string Customer { get; set; }
            }

            public Index()
            {
                Map = items => from item in items
                               let customer = LoadDocument<Customer1>(item.Number.ToString())
                               select new Result
                               {
                                   Id = item.Id,
                                   Customer = customer.Name
                               };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class CustomerNumberJsonConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var obj = (CustomerNumber)value;
                writer.WriteValue(obj.Number);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var number = (string)reader.Value;

                return new CustomerNumber { Number = number };
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(CustomerNumber) == objectType;
            }
        }
    }
}
