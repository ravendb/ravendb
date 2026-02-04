using System;
using System.Net;

namespace Raven.Client.Exceptions
{
    public class AiException : RavenException
    {
        public AiException(string message) : base(message)
        {
        }

        public AiException(string message, Exception e) : base(message, e)
        {
        }

        public string RequestId { get; set; }
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

    public class UnsuccessfulAiRequestException : AiException
    {
        public HttpStatusCode StatusCode { get; internal set; }

        public UnsuccessfulAiRequestException(string message, HttpStatusCode statusCode) : base(message)
        {
            StatusCode = statusCode;
        }

        public static void Throw(string message, HttpStatusCode statusCode, string requestId)
        {
            throw new UnsuccessfulAiRequestException($"Status Code: {statusCode}, Message: {message}", statusCode)
            {
                RequestId = requestId
            };
        }
    }

    public class TooManyRequestsException : UnsuccessfulAiRequestException
    {
        public TooManyRequestsException(string message) : base(message, (HttpStatusCode)429)
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

    public sealed class MissingAiAgentParameterException : RavenException
    {
        public MissingAiAgentParameterException(string message) : base(message)
        {
        }

        public MissingAiAgentParameterException(string message, Exception e) : base(message, e)
        {
        }
    }
}
