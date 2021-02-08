using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Documents;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter
{
    public class RequestTimeTracker : IDisposable
    {
        private readonly HttpContext _context;
        private readonly Logger _logger;
        private readonly DocumentDatabase _database;
        private readonly string _source;
        private readonly bool _doPerformanceHintIfTooLong;
        private readonly Stopwatch _sw;
        
        public RequestTimeTracker(HttpContext context, Logger logger, DocumentDatabase database, string source, bool doPerformanceHintIfTooLong = true)
        {
            _context = context;
            _logger = logger;
            _database = database;
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
            if (_database == null)
                return;// TODO: Sharding - fix this for queries
            if (_sw.Elapsed <= _database.Configuration.PerformanceHints.TooLongRequestThreshold.AsTimeSpan)
                return;

            if (_doPerformanceHintIfTooLong == false)
                return;
            
            try
            {
                _database
                    .NotificationCenter
                    .RequestLatency
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
