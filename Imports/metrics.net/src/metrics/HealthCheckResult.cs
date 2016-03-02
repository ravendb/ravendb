using System;
using System.Linq;
using System.Text;

namespace Metrics
{
    /// <summary>
    /// Result of a health check
    /// </summary>
    public struct HealthCheckResult
    {
        /// <summary>
        /// True if the check was successful, false if the check failed.
        /// </summary>
        public readonly bool IsHealthy;

        /// <summary>
        /// Status message of the check. A status can be provided for both healthy and unhealthy states.
        /// </summary>
        public readonly string Message;

        private HealthCheckResult(bool isHealthy, string message)
        {
            this.IsHealthy = isHealthy;
            this.Message = message;
        }

        /// <summary>
        /// Create a healthy status response.
        /// </summary>
        /// <returns>Healthy status response.</returns>
        public static HealthCheckResult Healthy()
        {
            return Healthy("OK");
        }

        /// <summary>
        /// Create a healthy status response.
        /// </summary>
        /// <param name="message">Status message.</param>
        /// <param name="values">Values to format the status message with.</param>
        /// <returns>Healthy status response.</returns>
        public static HealthCheckResult Healthy(string message, params object[] values)
        {
            var status = string.Format(message, values);
            return new HealthCheckResult(true, string.IsNullOrWhiteSpace(status) ? "OK" : status);
        }

        /// <summary>
        /// Create a unhealthy status response.
        /// </summary>
        /// <returns>Unhealthy status response.</returns>
        public static HealthCheckResult Unhealthy()
        {
            return Unhealthy("FAILED");
        }

        /// <summary>
        /// Create a unhealthy status response.
        /// </summary>
        /// <param name="message">Status message.</param>
        /// <param name="values">Values to format the status message with.</param>
        /// <returns>Unhealthy status response.</returns>
        public static HealthCheckResult Unhealthy(string message, params object[] values)
        {
            var status = string.Format(message, values);
            return new HealthCheckResult(false, string.IsNullOrWhiteSpace(status) ? "FAILED" : status);
        }

        /// <summary>
        /// Create a unhealthy status response.
        /// </summary>
        /// <param name="exception">Exception to use for reason.</param>
        /// <returns>Unhealthy status response.</returns>
        public static HealthCheckResult Unhealthy(Exception exception)
        {
            var status = string.Format("EXCEPTION: {0} - {1}", exception.GetType().Name, exception.Message);
            return HealthCheckResult.Unhealthy(status + Environment.NewLine + FormatStackTrace(exception));
        }

        private static string FormatStackTrace(Exception exception, int indent = 2)
        {
            StringBuilder builder = new StringBuilder();

            var aggregate = exception as AggregateException;
            var pad = new string(' ', indent * 2);
            if (aggregate != null)
            {
                builder.AppendFormat("{0}{1}: {2}" + Environment.NewLine, pad, exception.GetType().Name, exception.Message);

                foreach (var inner in aggregate.InnerExceptions)
                {
                    builder.AppendLine(FormatStackTrace(inner, indent + 2));
                }
            }
            else
            {
                builder.AppendFormat("{0}{1}: {2}" + Environment.NewLine, pad, exception.GetType().Name, exception.Message);

                if (exception.StackTrace != null)
                {
                    var stackLines = exception.StackTrace.Split('\n')
                        .Where(l => !string.IsNullOrWhiteSpace(l))
                        .Select(l => string.Concat(pad, l.Trim()));

                    builder.AppendLine(string.Join(Environment.NewLine, stackLines));
                }
                else
                {
                    builder.AppendLine(string.Concat(pad, "[No Stacktrace]"));
                }

                if (exception.InnerException != null)
                {
                    builder.AppendLine(FormatStackTrace(exception.InnerException, indent + 2));
                }
            }

            return builder.ToString();
        }
    }
}
