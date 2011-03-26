using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Json.Utilities;

namespace Raven.Json.Linq
{
    public abstract class RavenJToken
    {
        /// <summary>
        /// Gets the node type for this <see cref="RavenJToken"/>.
        /// </summary>
        /// <value>The type.</value>
        public abstract JTokenType Type { get; }

        /// <summary>
        /// Clones this object
        /// </summary>
        /// <returns>A cloned RavenJToken</returns>
        public abstract RavenJToken CloneToken();

        internal static RavenJToken FromObjectInternal(object o, JsonSerializer jsonSerializer)
        {
            ValidationUtils.ArgumentNotNull(o, "o");
            ValidationUtils.ArgumentNotNull(jsonSerializer, "jsonSerializer");

            RavenJToken token;
            using (var jsonWriter = new RavenJTokenWriter())
            {
                jsonSerializer.Serialize(jsonWriter, o);
                token = jsonWriter.Token;
            }

            return token;
        }

        /// <summary>
        /// Creates a <see cref="RavenJToken"/> from an object.
        /// </summary>
        /// <param name="o">The object that will be used to create <see cref="RavenJToken"/>.</param>
        /// <returns>A <see cref="RavenJToken"/> with the value of the specified object</returns>
        public static RavenJToken FromObject(object o)
        {
            return FromObjectInternal(o, new JsonSerializer());
        }

        /// <summary>
        /// Creates a <see cref="RavenJToken"/> from an object using the specified <see cref="JsonSerializer"/>.
        /// </summary>
        /// <param name="o">The object that will be used to create <see cref="RavenJToken"/>.</param>
        /// <param name="jsonSerializer">The <see cref="JsonSerializer"/> that will be used when reading the object.</param>
        /// <returns>A <see cref="RavenJToken"/> with the value of the specified object</returns>
        public static RavenJToken FromObject(object o, JsonSerializer jsonSerializer)
        {
            return FromObjectInternal(o, jsonSerializer);
        }

        /*
        /// <summary>
        /// Returns the indented JSON for this token.
        /// </summary>
        /// <returns>
        /// The indented JSON for this token.
        /// </returns>
        public override string ToString()
        {
            return ToString(Formatting.Indented);
        }

        /// <summary>
        /// Returns the JSON for this token using the given formatting and converters.
        /// </summary>
        /// <param name="formatting">Indicates how the output is formatted.</param>
        /// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
        /// <returns>The JSON for this token using the given formatting and converters.</returns>
        public string ToString(Formatting formatting, params JsonConverter[] converters)
        {
            using (var sw = new StringWriter(CultureInfo.InvariantCulture))
            {
                var jw = new JsonTextWriter(sw);
                jw.Formatting = formatting;

                WriteTo(jw, converters);

                return sw.ToString();
            }
        }*/

        /// <summary>
        /// Writes this token to a <see cref="JsonWriter"/>.
        /// </summary>
        /// <param name="writer">A <see cref="JsonWriter"/> into which this method will write.</param>
        /// <param name="converters">A collection of <see cref="JsonConverter"/> which will be used when writing the token.</param>
        //public abstract void WriteTo(JsonWriter writer, params JsonConverter[] converters);
    }
}
