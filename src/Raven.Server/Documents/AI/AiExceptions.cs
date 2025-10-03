using System;
using System.Net;
using System.Net.Http;
using System.Text;
using Sparrow.Json;

namespace Raven.Server.Documents.AI
{
    /// <summary>Base for all Gen‑AI service errors.</summary>
    public class AiException : Exception
    {
        public AiException(string message) : base(message)
        {
        }

        public AiException(string message, Exception e) : base(message, e)
        {
        }

        public required string RequestId { get; set; }
    }

    public sealed class RefusedToAnswerException(string message) : AiException(message)
    {
        public string Refusal;
        public string FinishReason;

        public static void Throw(string refusal, string responseContent, string finishReason, string requestId)
        {
            throw new RefusedToAnswerException($"The request was refused by the model: '{refusal}', response content: {responseContent}")
            {
                Refusal = refusal,
                FinishReason = finishReason,
                RequestId = requestId
            };
        }
    }

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

    public class UnsuccessfulRequestException : AiException
    {
        public HttpStatusCode StatusCode { get; }

        public UnsuccessfulRequestException(string message, HttpStatusCode statusCode) : base(message)
        {
            StatusCode = statusCode;
        }

        public static void Throw(string message, HttpStatusCode statusCode, string requestId)
        {
            throw new UnsuccessfulRequestException($"Status Code: {statusCode}, Message: {message}", statusCode)
            {
                RequestId = requestId
            };
        }
    }

    public class TooManyRequestsException : UnsuccessfulRequestException
    {

        public TooManyRequestsException(string message) : base(message, HttpStatusCode.TooManyRequests)
        {
        }
    }

    public sealed class RateLimitException(string message) : TooManyRequestsException(message)
    {
        public TimeSpan RetryAfter { get; set; }
    }


    public class InsufficientQuotaException(string message) : TooManyRequestsException(message)
    {
    }

    public sealed class TooManyTokensException(string message) : TooManyRequestsException(message)
    {
    }
}
