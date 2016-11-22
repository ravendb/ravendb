using System;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data
{
    public class OperationExceptionResult : IOperationResult
    {
        public string Message { get; set; }

        public string StackTrace { get; set; }

        public int StatusCode { get; set; } //http status code for special results like 409		

        public OperationExceptionResult()
        {
            // .ctor required for deserialization
        }

        public OperationExceptionResult(Exception exception, int statusCode = 500)
        {
            Message = exception.Message;
            StackTrace = ExceptionToString(exception);
            StatusCode = statusCode;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Message"] = Message,
                ["StackTrace"] = StackTrace,
                ["StatusCode"] = StatusCode
            };
        }

        private string ExceptionToString(Exception exception)
        {
            try
            {
                return exception.ToString(); //TODO: use toAsyncString()
            }
            catch (Exception)
            {
                return exception.ToString();
            }
        }
    }
}