// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Graphs;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Diagnostics.Tools.GCDump
{
    public static class EventPipeDotNetHeapDumper
    {
        internal static volatile bool eventPipeDataPresent = false;
        internal static volatile bool dumpComplete = false;

        /// <summary>
        /// Given a factory for creating an EventPipe session with the appropriate provider and keywords turned on,
        /// generate a GCHeapDump using the resulting events.  The correct keywords and provider name
        /// are given as input to the Func eventPipeEventSourceFactory.
        /// </summary>
        /// <param name="processID"></param>
        /// <param name="eventPipeEventSourceFactory">A delegate for creating and stopping EventPipe sessions</param>
        /// <param name="memoryGraph"></param>
        /// <param name="log"></param>
        /// <param name="dotNetInfo"></param>
        /// <returns></returns>
        public static bool DumpFromEventPipe(CancellationToken ct, int processID, MemoryGraph memoryGraph, TextWriter log, int timeout, DotNetHeapInfo dotNetInfo = null)
        {
            var startTicks = Stopwatch.GetTimestamp();
            Func<TimeSpan> getElapsed = () => TimeSpan.FromTicks(Stopwatch.GetTimestamp() - startTicks);

            var dumper = new DotNetHeapDumpGraphReader(log)
            {
                DotNetHeapInfo = dotNetInfo
            };

            try
            {
                TimeSpan lastEventPipeUpdate = getElapsed();
                bool fDone = false;
                log.WriteLine("{0,5:n1}s: Creating type table flushing task", getElapsed().TotalSeconds);

                using (EventPipeSessionController typeFlushSession = new EventPipeSessionController(processID, new List<EventPipeProvider> {
                    new EventPipeProvider("Microsoft-DotNETCore-SampleProfiler", EventLevel.Informational)
                }, false))
                {
                    log.WriteLine("{0,5:n1}s: Flushing the type table", getElapsed().TotalSeconds);
                    typeFlushSession.Source.AllEvents += (traceEvent) => {
                        if (!fDone)
                        {
                            fDone = true;
                            Task.Run(() => typeFlushSession.EndSession())
                                .ContinueWith(_ => typeFlushSession.Source.StopProcessing());
                        }
                    };

                    typeFlushSession.Source.Process();
                    log.WriteLine("{0,5:n1}s: Done flushing the type table", getElapsed().TotalSeconds);
                }


                // Start the providers and trigger the GCs.  
                log.WriteLine("{0,5:n1}s: Requesting a .NET Heap Dump", getElapsed().TotalSeconds);

                using EventPipeSessionController gcDumpSession = new EventPipeSessionController(processID, new List<EventPipeProvider> {
                    new EventPipeProvider("Microsoft-Windows-DotNETRuntime", EventLevel.Verbose, (long)(ClrTraceEventParser.Keywords.GCHeapSnapshot))
                });
                log.WriteLine("{0,5:n1}s: gcdump EventPipe Session started", getElapsed().TotalSeconds);

                int gcNum = -1;

                gcDumpSession.Source.Clr.GCStart += delegate (GCStartTraceData data)
                {
                    if (data.ProcessID != processID)
                    {
                        return;
                    }

                    eventPipeDataPresent = true;

                    if (gcNum < 0 && data.Depth == 2 && data.Type != GCType.BackgroundGC)
                    {
                        gcNum = data.Count;
                        log.WriteLine("{0,5:n1}s: .NET Dump Started...", getElapsed().TotalSeconds);
                    }
                };

                gcDumpSession.Source.Clr.GCStop += delegate (GCEndTraceData data)
                {
                    if (data.ProcessID != processID)
                    {
                        return;
                    }

                    if (data.Count == gcNum)
                    {
                        log.WriteLine("{0,5:n1}s: .NET GC Complete.", getElapsed().TotalSeconds);
                        dumpComplete = true;
                    }
                };

                gcDumpSession.Source.Clr.GCBulkNode += delegate (GCBulkNodeTraceData data)
                {
                    if (data.ProcessID != processID)
                    {
                        return;
                    }

                    eventPipeDataPresent = true;

                    if ((getElapsed() - lastEventPipeUpdate).TotalMilliseconds > 500)
                    {
                        log.WriteLine("{0,5:n1}s: Making GC Heap Progress...", getElapsed().TotalSeconds);
                    }

                    lastEventPipeUpdate = getElapsed();
                };

                if (memoryGraph != null)
                {
                    dumper.SetupCallbacks(memoryGraph, gcDumpSession.Source, processID.ToString());
                }

                // Set up a separate thread that will listen for EventPipe events coming back telling us we succeeded. 
                Task readerTask = Task.Run(() =>
                {
                    // cancelled before we began
                    if (ct.IsCancellationRequested)
                        return;
                    log.WriteLine("{0,5:n1}s: Starting to process events", getElapsed().TotalSeconds);
                    gcDumpSession.Source.Process();
                    log.WriteLine("{0,5:n1}s: EventPipe Listener dying", getElapsed().TotalSeconds);
                }, ct);

                for (; ; )
                {
                    if (ct.IsCancellationRequested)
                    {
                        log.WriteLine("{0,5:n1}s: Cancelling...", getElapsed().TotalSeconds);
                        break;
                    }

                    if (readerTask.Wait(100))
                    {
                        break;
                    }

                    if (!eventPipeDataPresent && getElapsed().TotalSeconds > 5)      // Assume it started within 5 seconds.  
                    {
                        log.WriteLine("{0,5:n1}s: Assume no .NET Heap", getElapsed().TotalSeconds);
                        break;
                    }

                    if (getElapsed().TotalSeconds > timeout)       // Time out after `timeout` seconds. defaults to 30s.
                    {
                        log.WriteLine("{0,5:n1}s: Timed out after {1} seconds", getElapsed().TotalSeconds, timeout);
                        break;
                    }

                    if (dumpComplete)
                    {
                        break;
                    }
                }

                var stopTask = Task.Run(() =>
                {
                    log.WriteLine("{0,5:n1}s: Shutting down gcdump EventPipe session", getElapsed().TotalSeconds);
                    gcDumpSession.EndSession();
                    log.WriteLine("{0,5:n1}s: gcdump EventPipe session shut down", getElapsed().TotalSeconds);
                }, ct);

                try
                {
                    while (!Task.WaitAll(new Task[] { readerTask, stopTask }, 1000))
                        log.WriteLine("{0,5:n1}s: still reading...", getElapsed().TotalSeconds);
                }
                catch (AggregateException ae) // no need to throw if we're just cancelling the tasks
                {
                    foreach (var e in ae.Flatten().InnerExceptions)
                    {
                        if (!(e is TaskCanceledException))
                        {
                            throw;
                        }
                    }
                }

                log.WriteLine("{0,5:n1}s: gcdump EventPipe Session closed", getElapsed().TotalSeconds);

                if (ct.IsCancellationRequested)
                    return false;

                if (eventPipeDataPresent)
                {
                    dumper.ConvertHeapDataToGraph();        // Finish the conversion.  
                }
            }
            catch (Exception e)
            {
                log.WriteLine($"{getElapsed().TotalSeconds,5:n1}s: [Error] Exception during gcdump: {e.ToString()}");
            }

            log.WriteLine("[{0,5:n1}s: Done Dumping .NET heap success={1}]", getElapsed().TotalSeconds, dumpComplete);

            return dumpComplete;
        }
    }

    internal class EventPipeSessionController : IDisposable
    {
        private List<EventPipeProvider> _providers;
        private DiagnosticsClient _client;
        private EventPipeSession _session;
        private EventPipeEventSource _source;
        private int _pid;

        public IReadOnlyList<EventPipeProvider> Providers => _providers.AsReadOnly();
        public EventPipeEventSource Source => _source;

        public EventPipeSessionController(int pid, List<EventPipeProvider> providers, bool requestRundown = true)
        {
            _pid = pid;
            _providers = providers;
            _client = new DiagnosticsClient(pid);
            _session = _client.StartEventPipeSession(providers, requestRundown, 1024);
            _source = new EventPipeEventSource(_session.EventStream);
        }

        public void EndSession()
        {
            _session.Stop();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _session?.Dispose();
                    _source?.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
