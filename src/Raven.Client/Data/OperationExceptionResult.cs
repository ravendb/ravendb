using System;
using System.Runtime.Serialization.Formatters;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Sparrow.Json.Parsing;

namespace Raven.Client.Data
{
    public class OperationExceptionResult : IOperationResult
    {
        public string Message { get; set; }

        public string StackTrace { get; set; }

        public OperationExceptionResult()
        {
            // .ctor required for deserialization
        }

        public OperationExceptionResult(Exception exception)
        {
            Message = exception.Message;
            StackTrace = ExceptionToString(exception);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue(GetType())
            {
                ["Message"] = Message,
                ["StackTrace"] = StackTrace
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