using System;
using System.Net;

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
    }

    public sealed class UnexpectedResponseException : AiException
    {
        public UnexpectedResponseException(string message) : base(message)
        {
        }
        public UnexpectedResponseException(string message, Exception e) : base(message, e)
        {
        }
    }

    public class UnsuccessfulRequestException : AiException
    {
        public HttpStatusCode StatusCode { get; }

        public UnsuccessfulRequestException(string message, HttpStatusCode statusCode) : base(message)
        {
            StatusCode = statusCode;
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
