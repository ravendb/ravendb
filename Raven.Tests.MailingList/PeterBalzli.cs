// -----------------------------------------------------------------------
//  <copyright file="PeterBalzli.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using FluentAssertions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class PeterBalzli : RavenTest
    {
        private EmbeddableDocumentStore _documentStore { get; set; }

        public void Setup()
        {
            _documentStore = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true
            };

            _documentStore.Conventions.CustomizeJsonSerializer = serializers => serializers.Converters.Add(new CustomerNumberJsonConverter());
            _documentStore.Configuration.Storage.Voron.AllowOn32Bits = true;
            _documentStore.Initialize();
            _documentStore.ExecuteIndex(new Index());
        }

        [Fact]
        public void Test()
        {
            Setup();

            using (var session = _documentStore.OpenSession())
            {
                session.Store(new Item() { Id = "1" });
                session.SaveChanges();
            }

            using (var session = _documentStore.OpenSession())
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

    public class Item
    {
        public string Id { get; set; }
        public CustomerNumber Number { get; set; }
    }

    public class Customer1
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class CustomerNumber
    {
        public string Number { get; set; }
    }

    public class Index : AbstractIndexCreationTask<Item, Index.Result>
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

    public class CustomerNumberJsonConverter : JsonConverter
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
