
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using Metrics.Logging;
namespace Metrics
{
    public class MetricsErrorHandler
    {

        private static readonly MetricsErrorHandler handler = new MetricsErrorHandler();

        private readonly ConcurrentBag<Action<Exception, string>> handlers = new ConcurrentBag<Action<Exception, string>>();

        private static readonly bool IsMono = Type.GetType("Mono.Runtime") != null;

        private MetricsErrorHandler()
        {
            this.AddHandler((x, msg) => Trace.TraceError("Metrics: Unhandled exception in Metrics.NET Library " + x.ToString()));
        }

        internal static MetricsErrorHandler Handler { get { return handler; } }

        internal void AddHandler(Action<Exception> handler)
        {
            AddHandler((x, msg) => handler(x));
        }

        internal void AddHandler(Action<Exception, string> handler)
        {
            handlers.Add(handler);
        }

        internal void ClearHandlers()
        {
            while (!this.handlers.IsEmpty)
            {
                Action<Exception, string> item;
                this.handlers.TryTake(out item);
            }
        }

        private void InternalHandle(Exception exception, string message)
        {
            foreach (var handler in this.handlers)
            {
                try
                {
                    handler(exception, message);
                }
                catch
                {
                    // error handler throw-ed on us, hope you have a debugger attached.
                }
            }
        }

        public static void Handle(Exception exception)
        {
            MetricsErrorHandler.Handle(exception, string.Empty);
        }

        public static void Handle(Exception exception, string message)
        {
            MetricsErrorHandler.handler.InternalHandle(exception, message);
        }
    }
}
