using System;
using System.IO;
using System.Reflection;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;
using MemberInfoExtensions = Raven.Abstractions.Extensions.MemberInfoExtensions;

namespace NewBlittable.Tests
{
    public static class StringExtentions
    {
        public static string ToJsonString(this object self)
        {
            var jsonSerializer = new JsonSerializer();
            var stringWriter = new StringWriter();
            var jsonWriter = new JsonTextWriter(stringWriter);
            jsonSerializer.Serialize(jsonWriter, self);

            return stringWriter.ToString();
        }
    }
}
