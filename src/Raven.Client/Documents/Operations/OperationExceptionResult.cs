using System;
using System.Net;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations
{
    public class OperationExceptionResult : IOperationResult
    {
        public string Type { get; set; }

        public string Message { get; set; }

        public string Error { get; set; }

        public HttpStatusCode StatusCode { get; set; } //http status code for special results like 409		

        public OperationExceptionResult()
        {
            // .ctor required for deserialization
        }

        public OperationExceptionResult(Exception exception, HttpStatusCode statusCode, bool shouldBePersistent)
        {
            ShouldPersist = shouldBePersistent;
            Type = exception.GetType().FullName;
            Message = exception.Message;
            Error = ExceptionToString(exception);
            StatusCode = statusCode;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                [nameof(Type)] = Type,
                [nameof(Message)] = Message,
                [nameof(Error)] = Error,
                [nameof(StatusCode)] = (int)StatusCode
            };
        }

        private static string ExceptionToString(Exception exception)
        {
            try
            {
                return exception.ToString();
            }
            catch (Exception)
            {
                return exception.ToString();
            }
        }

        public bool ShouldPersist { get; }

        bool IOperationResult.CanMerge => false;

        void IOperationResult.MergeWith(IOperationResult result)
        {
            throw new NotImplementedException();
        }
    }
}
