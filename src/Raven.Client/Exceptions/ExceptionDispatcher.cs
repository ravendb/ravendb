using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Exceptions
{
    internal static class ExceptionDispatcher
    {
        public class ExceptionSchema
        {
            public string Url { get; set; }

            public string Type { get; set; }

            public string Message { get; set; }

            public string Error { get; set; }
        }

        public static Exception Get(ExceptionSchema schema, HttpStatusCode code, Exception inner) => Get(schema.Message, schema.Error, schema.Type, code, inner);

        public static Exception Get(string message, string error, string typeAsString, HttpStatusCode code, Exception inner = null)
        {
            if (code == HttpStatusCode.Conflict)
            {
                if (typeAsString.Contains(nameof(DocumentConflictException)))
                    return DocumentConflictException.From(message);

                return new ConcurrencyException(message);
            }

            // We throw the same error for different status codes: GatewayTimeout,RequestTimeout,BadGateway,ServiceUnavailable.
            var error = $"{schema.Error}{Environment.NewLine}" +
                        $"The server at {schema.Url} responded with status code: {code}.";

            var type = GetType(typeAsString);
            if (type == null)
                return new RavenException(error, inner);

            Exception exception;
            try
            {
                exception = (Exception)Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, null, new[] { error }, null, null);
            }
            catch (Exception)
            {
                return new RavenException(error);
            }

            if (typeof(RavenException).IsAssignableFrom(type) == false)
                return new RavenException(error, exception);

            return exception;
        }

        public static async Task Throw(JsonOperationContext context, HttpResponseMessage response, Action<StringBuilder> additionalErrorInfo = null)
        {
            if (response == null)
                throw new ArgumentNullException(nameof(response));

            using (var stream = await RequestExecutor.ReadAsStreamUncompressedAsync(response).ConfigureAwait(false))
            using (var json = await GetJson(context, response, stream).ConfigureAwait(false))
            {
                var schema = GetExceptionSchema(response, json);

                if (response.StatusCode == HttpStatusCode.Conflict)
                {
                    ThrowConflict(schema, json);
                    return;
                }

                var type = GetType(schema.Type);
                if (type == null)
                    throw RavenException.Generic(schema.Error, json);

                Exception exception;
                try
                {
                    string message;
                    if (additionalErrorInfo != null)
                    {
                        var sb = new StringBuilder(schema.Error);
                        additionalErrorInfo(sb);
                        message = sb.ToString();
                    }
                    else
                    {
                        message = schema.Error;
                    }
                    exception = (Exception)Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, null, new[] { message }, null, null);
                }
                catch (Exception)
                {
                    throw RavenException.Generic(schema.Error, json);
                }

                if (typeof(RavenException).IsAssignableFrom(type) == false)
                    throw new RavenException(schema.Error, exception);

                if (type == typeof(IndexCompilationException))
                {
                    var indexCompilationException = (IndexCompilationException)exception;
                    json.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty), out indexCompilationException.IndexDefinitionProperty);
                    json.TryGet(nameof(IndexCompilationException.ProblematicText), out indexCompilationException.ProblematicText);

                    throw indexCompilationException;
                }

                throw exception;
            }
        }

        private static void ThrowConflict(ExceptionSchema schema, BlittableJsonReaderObject json)
        {
            if (schema.Type.Contains(nameof(DocumentConflictException))) // temporary!
                throw DocumentConflictException.From(json);

            throw new ConcurrencyException(schema.Message);
        }

        public static Type GetType(string typeAsString)
        {
            return Type.GetType(typeAsString, throwOnError: false);
        }

        private static ExceptionSchema GetExceptionSchema(HttpResponseMessage response, BlittableJsonReaderObject json)
        {
            ExceptionSchema schema;
            try
            {
                schema = JsonDeserializationClient.ExceptionSchema(json);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot deserialize the {response.StatusCode} response. JSON: {json}", e);
            }

            if (schema == null)
                throw new BadResponseException($"After deserialization the {response.StatusCode} response is null. JSON: {json}");

            if (schema.Message == null)
                throw new BadResponseException($"After deserialization the {response.StatusCode} response property '{nameof(schema.Message)}' is null. JSON: {json}");

            if (schema.Error == null)
                throw new BadResponseException($"After deserialization the {response.StatusCode} response property '{nameof(schema.Error)}' is null. JSON: {json}");

            if (schema.Type == null)
                throw new BadResponseException($"After deserialization the {response.StatusCode} response property '{nameof(schema.Type)}' is null. JSON: {json}");

            return schema;
        }

        private static async Task<BlittableJsonReaderObject> GetJson(JsonOperationContext context, HttpResponseMessage response, Stream stream)
        {
            BlittableJsonReaderObject json;
            try
            {
                json = await context.ReadForMemoryAsync(stream, "error/response").ConfigureAwait(false);
            }
            catch (Exception e)
            {
                string content = null;
                if (stream.CanSeek)
                {
                    stream.Position = 0;
                    using (var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4096, leaveOpen: true))
                        content = await reader.ReadToEndAsync().ConfigureAwait(false);
                }

                if (content != null)
                    content = $"Content: {content}";

                throw new InvalidOperationException($"Cannot parse the '{response.StatusCode}' response. {content}", e);
            }

            return json;
        }
    }
}
