using System;
using System.Linq;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;
using JsonConverter = Raven.Imports.Newtonsoft.Json.JsonConverter;
using JsonReader = Raven.Imports.Newtonsoft.Json.JsonReader;
using JsonSerializer = Raven.Imports.Newtonsoft.Json.JsonSerializer;
using JsonWriter = Raven.Imports.Newtonsoft.Json.JsonWriter;

namespace Raven.Tests.Issues
{
    public class QueryTests : RavenTestBase
    {
        [Fact]
        public void NullableToNonNullableSelectFields()
        {
            using (var store = NewDocumentStore())
            {

                store.Conventions.CustomizeJsonSerializer += serializer =>
                {
                    serializer.Converters.Add(new IntNullableConverter());
                    foreach (var converter in Default.Converters)
                    {
                        serializer.Converters.Add(converter);
                    }
                    serializer.Converters.Freeze();
                };
                var index = new Email_BySubject();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Email { Subject = "TestSubject", ConversationCount = null });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var email = session.Query<Email, Email_BySubject>()
                        .ProjectFromIndexFieldsInto<EmailProjection>()
                        .FirstOrDefault();
                    Assert.Equal("TestSubject", email.Subject);
                    Assert.Equal(0, email.ConversationCount);
                }
            }
        }

        public class IntNullableConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(value);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.Value == null)
                    return 0;
                if (reader.Value is long)
                    return (int) (long) reader.Value;
                return (int)reader.Value ;
            }

            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof (int);
            }
        }

        private class EmailProjection
        {
            public string Subject { get; set; }
            public int ConversationCount { get; set; }
        }

        private class Email
        {
            public string Subject { get; set; }
            public int? ConversationCount { get; set; }
        }

        private class Email_BySubject : AbstractIndexCreationTask<Email>
        {
            public Email_BySubject()
            {
                Map = emails => from email in emails
                                select new
                                {
                                    email.Subject,
                                    email.ConversationCount
                                };

                Store(x => x.Subject, FieldStorage.Yes);
                //Store(x => x.ConversationCount, FieldStorage.Yes);
            }
        }
    }
}

