using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    /// <summary>
    /// An advanced patch request for a specified document (using JavaScript)
    /// </summary>
    public class PatchRequest
    {
        /// <summary>
        /// JavaScript function to use to patch a document
        /// </summary>
        /// <value>The type.</value>
        public string Script { get; set; }

        /// <summary>
        /// Additional arguments passed to JavaScript function from Script.
        /// </summary>
        public Dictionary<string, object> Values { get; set; }

        public PatchRequest()
        {
            Values = new Dictionary<string, object>();
        }

        protected bool Equals(PatchRequest other)
        {
            if (other == null)
                return false;
            return string.Equals(Script, other.Script) && Values.Keys.SequenceEqual(other.Values.Keys);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return Equals(obj as PatchRequest);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return Values.Keys.Aggregate(Script.GetHashCode()*397, (i, s) => i*397 ^ s.GetHashCode());
            }
        }

        public DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context)
        {
            var json = new DynamicJsonValue
            {
                [nameof(Script)] = Script
            };

            if (Values != null)
                json[nameof(Values)] = TypeConverter.ToBlittableSupportedType(Values, conventions, context);

            return json;
        }
    }
}
