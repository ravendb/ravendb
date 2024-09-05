using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Compilation;
using Raven.Client.Http;
using Raven.Client.Http.Behaviors;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Exceptions
{
    internal static class ExceptionDispatcher
    {
        internal sealed class ExceptionSchema
        {
            public string Url { get; set; }

            public string Type { get; set; }

            public string Message { get; set; }

            public string Error { get; set; }
        }

        public static Exception Get(ExceptionSchema schema, HttpStatusCode code, Exception inner = null)
        {
            var message = schema.Message;
            var typeAsString = schema.Type;

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

        public static Exception Get(BlittableJsonReaderObject json, HttpStatusCode code, Exception inner = null)
        {
            var schema = GetExceptionSchema(code, json);

            var exception = Get(schema, code, inner);

            FillException(exception, json);

            return exception;
        }

        public static async Task Throw(JsonOperationContext context, HttpResponseMessage response, AbstractCommandResponseBehavior.CommandUnsuccessfulResponseBehavior unsuccessfulResponseBehavior)
        {
            if (response == null)
                throw new ArgumentNullException(nameof(response));

#if NETSTANDARD2_0
            using (var stream = await RequestExecutor.ReadAsStreamUncompressedAsync(response).ConfigureAwait(false))
#else
            await using (var stream = await RequestExecutor.ReadAsStreamUncompressedAsync(response).ConfigureAwait(false))
#endif
            using (var json = await GetJson(context, response, stream).ConfigureAwait(false))
            {
                var schema = GetExceptionSchema(response.StatusCode, json);

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
                    var message = schema.Error;

                    exception = (Exception)Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public, null, new[] { message }, null, null);
                }
                catch (Exception)
                {
                    throw RavenException.Generic(schema.Error, json);
                }

                if (unsuccessfulResponseBehavior == AbstractCommandResponseBehavior.CommandUnsuccessfulResponseBehavior.WrapException && 
                    typeof(RavenException).IsAssignableFrom(type) == false)
                    throw new RavenException(schema.Error, exception);

                FillException(exception, json);

                throw exception;
            }
        }

        private static void FillException(Exception exception, BlittableJsonReaderObject json)
        {
            switch (exception)
            {
                case IndexCompilationException indexCompilationException:
                    json.TryGet(nameof(IndexCompilationException.IndexDefinitionProperty), out indexCompilationException.IndexDefinitionProperty);
                    json.TryGet(nameof(IndexCompilationException.ProblematicText), out indexCompilationException.ProblematicText);
                    break;
                case RavenTimeoutException timeoutException:
                    json.TryGet(nameof(RavenTimeoutException.FailImmediately), out timeoutException.FailImmediately);
                    break;
                case BackupAlreadyRunningException backupAlreadyRunningException:
                    json.TryGet(nameof(BackupAlreadyRunningException.OperationId), out backupAlreadyRunningException.OperationId);
                    json.TryGet(nameof(BackupAlreadyRunningException.NodeTag), out backupAlreadyRunningException.NodeTag);
                    break;
            }
        }

        private static void ThrowConflict(ExceptionSchema schema, BlittableJsonReaderObject json)
        {
            if (schema.Type.Contains(nameof(DocumentConflictException))) // temporary!
                throw DocumentConflictException.From(json);

            string expectedCv, actualCv, docId;
            if (schema.Type.Contains(nameof(ClusterTransactionConcurrencyException)))
            {
                var ctxConcurrencyException = new ClusterTransactionConcurrencyException(schema.Message);

                if (json.TryGet(nameof(ClusterTransactionConcurrencyException.Id), out docId))
                    ctxConcurrencyException.Id = docId;

                if (json.TryGet(nameof(ClusterTransactionConcurrencyException.ExpectedChangeVector), out expectedCv))
                    ctxConcurrencyException.ExpectedChangeVector = expectedCv;

                if (json.TryGet(nameof(ClusterTransactionConcurrencyException.ActualChangeVector), out actualCv))
                    ctxConcurrencyException.ActualChangeVector = actualCv;

                if (json.TryGet(nameof(ClusterTransactionConcurrencyException.ConcurrencyViolations), out BlittableJsonReaderArray violations) == false)
                    throw ctxConcurrencyException;

                ctxConcurrencyException.ConcurrencyViolations = new ClusterTransactionConcurrencyException.ConcurrencyViolation[violations.Length];

                for (var i = 0; i < violations.Length; i++)
                {
                    if (!(violations[i] is BlittableJsonReaderObject violation))
                        continue;

                    var current = ctxConcurrencyException.ConcurrencyViolations[i] = new ClusterTransactionConcurrencyException.ConcurrencyViolation();

                    if (violation.TryGet(nameof(ClusterTransactionConcurrencyException.ConcurrencyViolation.Id), out string id))
                        current.Id = id;

                    if (violation.TryGet(nameof(ClusterTransactionConcurrencyException.ConcurrencyViolation.Type), out ClusterTransactionConcurrencyException.ViolationOnType type))
                        current.Type = type;

                    if (violation.TryGet(nameof(ClusterTransactionConcurrencyException.ConcurrencyViolation.Expected), out long expected))
                        current.Expected = expected;

                    if (violation.TryGet(nameof(ClusterTransactionConcurrencyException.ConcurrencyViolation.Actual), out long actual))
                        current.Actual = actual;
                }

                throw ctxConcurrencyException;
            }

            var concurrencyException = new ConcurrencyException(schema.Message);

            if (json.TryGet(nameof(ConcurrencyException.Id), out docId))
                concurrencyException.Id = docId;
            if (json.TryGet(nameof(ConcurrencyException.ExpectedChangeVector), out expectedCv))
                concurrencyException.ExpectedChangeVector = expectedCv;
            if (json.TryGet(nameof(ConcurrencyException.ActualChangeVector), out actualCv))
                concurrencyException.ActualChangeVector = actualCv;

            throw concurrencyException;
        }

        public static Type GetType(string typeAsString)
        {
            return Type.GetType(typeAsString, throwOnError: false);
        }

        private static ExceptionSchema GetExceptionSchema(HttpStatusCode code, BlittableJsonReaderObject json)
        {
            ExceptionSchema schema;
            try
            {
                schema = JsonDeserializationClient.ExceptionSchema(json);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Cannot deserialize the {code} response. JSON: {json}", e);
            }

            if (schema == null)
                throw new BadResponseException($"After deserialization the {code} response is null. JSON: {json}");

            if (schema.Message == null)
                throw new BadResponseException($"After deserialization the {code} response property '{nameof(schema.Message)}' is null. JSON: {json}");

            if (schema.Error == null)
                throw new BadResponseException($"After deserialization the {code} response property '{nameof(schema.Error)}' is null. JSON: {json}");

            if (schema.Type == null)
                throw new BadResponseException($"After deserialization the {code} response property '{nameof(schema.Type)}' is null. JSON: {json}");

            return schema;
        }

        private static async Task<BlittableJsonReaderObject> GetJson(JsonOperationContext context, HttpResponseMessage response, Stream stream)
        {
            var ms = context.CheckoutMemoryStream();

            BlittableJsonReaderObject json;

            try
            {
                // copying the error stream so we can read it as string if we fail to parse it
                await stream.CopyToAsync(ms).ConfigureAwait(false);
                ms.Position = 0;
                json = await context.ReadForMemoryAsync(ms, "error/response").ConfigureAwait(false);
            }
            catch (Exception e)
            {
                string content = "Content: ";
                ms.Position = 0;
                using (var reader = new StreamReader(ms, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 4096, leaveOpen: true))
                    content += await reader.ReadToEndAsync().ConfigureAwait(false);

                throw new InvalidOperationException($"Cannot parse the '{response.StatusCode}' response. {content}", e);
            }
            finally
            {
                context.ReturnMemoryStream(ms);
            }

            return json;
        }
    }
}
