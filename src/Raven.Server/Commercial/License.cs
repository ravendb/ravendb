using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class License
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public List<string> Keys { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id.ToString(),
                [nameof(Name)] = Name,
                [nameof(Keys)] = new DynamicJsonArray(Keys)
            };
        }
    }
}