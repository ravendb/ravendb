// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1716.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4196 : NoDisposalNeeded
    {
        [Theory] //                      -->|<--- diff is here
        [InlineData("{\"Data\":\"AQIDTWFu\",\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDTWFu\",\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDTWFu\",\"Metadata\":null}")]
        public void CanReadBytesAsStreamNoPadding(string text)
        {
            ParseAndValidate(text, 6);
        }

        [Theory] //                      -->|<--- diff is here
        [InlineData("{\"Data\":\"AQIDBAU=\",\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDBAU=\",\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDBAU=\",\"Metadata\":null}")]
        public void CanReadBytesAsStreamSinglePadding(string text)
        {
            ParseAndValidate(text, 5);
        }

        [Theory] //                      -->|<--- diff is here
        [InlineData("{\"Data\":\"AQIDTQ==\",\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDTQ==\",\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\"Metadata\":null}")]
        [InlineData("{\"Data\":\"AQIDTQ==\",\"Metadata\":null}")]
        public void CanReadBytesAsStreamDoublePadding(string text)
        {
            ParseAndValidate(text, 4);
        }

        private static void ParseAndValidate(string text, int expectedLength)
        {
            var reader = new StringReader(text);
            var jsonTextReader = new JsonTextReader(reader);

            Assert.True(jsonTextReader.Read());
            Assert.Equal(JsonToken.StartObject, jsonTextReader.TokenType);

            Assert.True(jsonTextReader.Read());
            Assert.Equal(JsonToken.PropertyName, jsonTextReader.TokenType);
            Assert.Equal("Data", jsonTextReader.Value);

            var memoryStream = jsonTextReader.ReadBytesAsStream();
            Assert.Equal(expectedLength, memoryStream.Length);

            Assert.True(jsonTextReader.Read());
            Assert.Equal(JsonToken.PropertyName, jsonTextReader.TokenType);
            Assert.Equal("Metadata", jsonTextReader.Value);

            Assert.True(jsonTextReader.Read());
            Assert.Equal(JsonToken.Null, jsonTextReader.TokenType);

            Assert.True(jsonTextReader.Read());
            Assert.Equal(JsonToken.EndObject, jsonTextReader.TokenType);

            Assert.False(jsonTextReader.Read());
        }
    }
}
