using System;

namespace metrics
{
    /// <summary>
    /// A template class for an encapsulated service health check
    /// </summary>
    public class HealthCheck
    {
        public static Result Healthy { get { return Result.Healthy; } }
        public static Result Unhealthy(string message) { return Result.Unhealthy(message); }
        public static Result Unhealthy(Exception error) { return Result.Unhealthy(error); } 

        private readonly Func<Result> _check;

        public String Name { get; private set; }

        public HealthCheck(string name, Func<Result> check)
        {
            Name = name;
            _check = check;
        }

        public Result Execute()
        {
            try
            {
                return _check();
            }
            catch (Exception e)
            {
                return Result.Unhealthy(e);
            }
        }

        public sealed class Result
        {
            private static readonly Result _healthy = new Result(true, null, null);

            public static Result Healthy { get { return _healthy; } }

            public static Result Unhealthy(string errorMessage)
            {
                return new Result(false, errorMessage, null);
            }

            public static Result Unhealthy(Exception error)
            {
                return new Result(false, error.Message, error);
            }

            public string Message { get; private set; }
            
            public Exception Error { get; private set; }

            public bool IsHealthy { get; private set; }
            
            private Result(bool isHealthy, string message, Exception error)
            {
                IsHealthy = isHealthy;
                Message = message;
                Error = error;
            }
        }
    }
}