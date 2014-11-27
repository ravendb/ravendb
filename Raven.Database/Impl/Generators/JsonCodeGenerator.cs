using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Impl.Generators
{
    public class JsonCodeGenerator
    {
        private string language;

        public JsonCodeGenerator( string lang)
        {
            if (string.IsNullOrWhiteSpace(lang))
                throw new ArgumentNullException("lang");

            this.language = lang;
        }

        public string Execute(JsonDocument document)
        {
            if (document == null)
                throw new ArgumentNullException("document");

            dynamic d = JsonExtensions.JsonDeserialization<dynamic>(document.DataAsJson);
            
            throw new NotImplementedException();
        }
    }
}
