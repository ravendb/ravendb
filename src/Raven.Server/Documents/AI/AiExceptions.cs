using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Http;
using System.Text;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Server.Documents.AI
{
    public sealed class UnexpectedResponseException : AiException
    {
        public UnexpectedResponseException(string message) : base(message)
        {
        }
        public UnexpectedResponseException(string message, Exception e) : base(message, e)
        {
        }

        public static UnexpectedResponseException Create(string message, HttpResponseMessage response, BlittableJsonReaderObject content, Exception e = null)
            => Create(message, response, content.ToString(), e);

        public static UnexpectedResponseException Create(string message, HttpResponseMessage response, string content, Exception e = null)
        {
            var sb = new StringBuilder();
            sb.Append(message).AppendLine(".")
                .Append("Status Code: ").Append(response.StatusCode).AppendLine()
                .AppendLine("Response:").AppendLine(response.ToString())
                .AppendLine("Content:").AppendLine(content);

            return new UnexpectedResponseException(sb.ToString(), e)
            {
                RequestId = ChatCompletionClient.GetRequestId(response.Headers)
            };
        }
    }
}
