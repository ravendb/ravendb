using System;
using System.Collections.Generic;
using System.Linq;
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

        public override bool Equals(object obj)
        {
            return obj is License otherLicense &&
                   Id == otherLicense.Id &&
                   Name == otherLicense.Name &&
                   Keys.All(otherLicense.Keys.Contains);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Id != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Id) : 0) * 397) ^ 
                       (Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0) ^
                       (Keys != null ? Keys.GetHashCode() : 0);
            }
        }

        public override string ToString()
        {
            return $"Id: {Id}, Name: {Name}, Keys: {string.Join(",", Keys)}";
        }
    }
}
