using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Config;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class RequestTimeTracker : IDisposable
    {
        private readonly HttpContext _context;
        private readonly Logger _logger;
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;
        private readonly RavenConfiguration _configuration;
        private readonly string _source;
        private readonly bool _doPerformanceHintIfTooLong;
        private readonly Stopwatch _sw;
        
        public RequestTimeTracker(HttpContext context, Logger logger, AbstractDatabaseNotificationCenter notificationCenter, RavenConfiguration configuration, string source, bool doPerformanceHintIfTooLong = true)
        {
            _context = context;
            _logger = logger;
            _notificationCenter = notificationCenter;
            _configuration = configuration;
            _source = source;
            _doPerformanceHintIfTooLong = doPerformanceHintIfTooLong;
            
            _sw = Stopwatch.StartNew();

            _context.Response.OnStarting(state =>
            {
                _sw.Stop();
                var httpContext = (HttpContext)state;
                httpContext.Response.Headers.Add(Constants.Headers.RequestTime, _sw.ElapsedMilliseconds.ToString());
                return Task.CompletedTask;
            }, _context);
        }
        
        public string Query { get; set; }

        public void Dispose()
        {
            if (_sw.Elapsed <= _configuration.PerformanceHints.TooLongRequestThreshold.AsTimeSpan)
                return;

            if (_doPerformanceHintIfTooLong == false)
                return;
            
            try
            {
                _notificationCenter.RequestLatency
                    .AddHint(_sw.ElapsedMilliseconds, _source, Query);
            }
            catch (Exception e)
            {
                //precaution - should never arrive here
                if (_logger.IsInfoEnabled)
                    _logger.Info(
                        $"Failed to write request time in response headers. This is not supposed to happen and is probably a bug. The request path was: {_context.Request.Path}",
                        e);

                throw;
            }
        }
    }
}
