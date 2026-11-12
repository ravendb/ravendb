// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Graphs;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Parsers.Clr;

namespace Microsoft.Diagnostics.Tools.GCDump
{
    public static class EventPipeDotNetHeapDumper
    {
        /// <summary>
        /// Given a nettrace file from a EventPipe session with the appropriate provider and keywords turned on,
        /// generate a GCHeapDump using the resulting events.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="memoryGraph"></param>
        /// <param name="log"></param>
        /// <param name="dotNetInfo"></param>
        /// <returns></returns>
        public static bool DumpFromEventPipeFile(string path, MemoryGraph memoryGraph, TextWriter log, DotNetHeapInfo dotNetInfo)
        {
            DateTime start = DateTime.Now;
            Func<TimeSpan> getElapsed = () => DateTime.Now - start;
            DumpState dumpState = new();
            bool success = false;

            DotNetHeapDumpGraphReader dumper = new(log)
            {
                DotNetHeapInfo = dotNetInfo
            };

            try
            {
                TimeSpan lastEventPipeUpdate = getElapsed();

                int gcNum = -1;

                EventPipeEventSource source = new(path);

                source.Clr.GCStart += delegate (GCStartTraceData data)
                {
                    dumpState.EventPipeDataPresent = true;

                    if (gcNum < 0 && data.Depth == 2 && data.Type != GCType.BackgroundGC)
                    {
                        gcNum = data.Count;
                        log.WriteLine("{0,5:n1}s: .NET Dump Started...", getElapsed().TotalSeconds);
                    }
                };

                source.Clr.GCStop += delegate (GCEndTraceData data)
                {
                    if (data.Count == gcNum)
                    {
                        log.WriteLine("{0,5:n1}s: .NET GC Complete.", getElapsed().TotalSeconds);
                        dumpState.DumpComplete = true;
                    }
                };

                source.Clr.GCBulkNode += delegate (GCBulkNodeTraceData data)
                {
                    dumpState.EventPipeDataPresent = true;

                    if ((getElapsed() - lastEventPipeUpdate).TotalMilliseconds > 500)
                    {
                        log.WriteLine("{0,5:n1}s: Making GC Heap Progress...", getElapsed().TotalSeconds);
                    }

                    lastEventPipeUpdate = getElapsed();
                };

                if (memoryGraph != null)
                {
                    dumper.SetupCallbacks(memoryGraph, source);
                }

                log.WriteLine("{0,5:n1}s: Starting to process events", getElapsed().TotalSeconds);
                source.Process();
                log.WriteLine("{0,5:n1}s: Finished processing events", getElapsed().TotalSeconds);

                if (dumpState.EventPipeDataPresent)
                {
                    dumper.ConvertHeapDataToGraph();
                    success = dumpState.DumpComplete;
                }
            }
            catch (Exception e)
            {
                log.WriteLine($"{getElapsed().TotalSeconds,5:n1}s: [Error] Exception processing events: {e}");
            }

            log.WriteLine("[{0,5:n1}s: Done Dumping .NET heap success={1}]", getElapsed().TotalSeconds, success);

            return success;
        }

        /// <summary>
        /// Given a factory for creating an EventPipe session with the appropriate provider and keywords turned on,
        /// generate a GCHeapDump using the resulting events.  The correct keywords and provider name
        /// are given as input to the Func eventPipeEventSourceFactory.
        /// </summary>
        /// <param name="processId"></param>
        /// <param name="memoryGraph"></param>
        /// <param name="log">A writer that supports concurrent writes. This method may log from multiple tasks.</param>
        /// <param name="timeout"></param>
        /// <param name="dotNetInfo"></param>
        /// <returns></returns>
        public static bool DumpFromEventPipe(CancellationToken ct, int processId, MemoryGraph memoryGraph, TextWriter log, int timeout, DotNetHeapInfo dotNetInfo)
        {
            DateTime start = DateTime.Now;
            Func<TimeSpan> getElapsed = () => DateTime.Now - start;
            DumpState dumpState = new();
            bool success = false;

            DotNetHeapDumpGraphReader dumper = new(log)
            {
                DotNetHeapInfo = dotNetInfo
            };

            try
            {
                TimeSpan lastEventPipeUpdate = getElapsed();
                bool fDone = false;
                log.WriteLine("{0,5:n1}s: Creating type table flushing task", getElapsed().TotalSeconds);

                using (EventPipeSessionController typeFlushSession = new(processId, new List<EventPipeProvider> {
                    new("Microsoft-DotNETCore-SampleProfiler", EventLevel.Informational)
                }, false))
                {
                    log.WriteLine("{0,5:n1}s: Flushing the type table", getElapsed().TotalSeconds);
                    typeFlushSession.Source.AllEvents += (traceEvent) => {
                        if (!fDone)
                        {
                            fDone = true;
                            Task.Run(
                                () => {
                                    typeFlushSession.EndSession();
                                    typeFlushSession.Source.StopProcessing();
                                });
                        }
                    };

                    typeFlushSession.Source.Process();
                    log.WriteLine("{0,5:n1}s: Done flushing the type table", getElapsed().TotalSeconds);
                }


                // Start the providers and trigger the GCs.
                log.WriteLine("{0,5:n1}s: Requesting a .NET Heap Dump", getElapsed().TotalSeconds);

                using EventPipeSessionController gcDumpSession = new(processId, new List<EventPipeProvider> {
                    new("Microsoft-Windows-DotNETRuntime", EventLevel.Verbose, (long)(ClrTraceEventParser.Keywords.GCHeapSnapshot))
                }, bufferingMode: EventPipeBufferingMode.Block);
                log.WriteLine("{0,5:n1}s: gcdump EventPipe Session started", getElapsed().TotalSeconds);

                int gcNum = -1;

                gcDumpSession.Source.Clr.GCStart += delegate (GCStartTraceData data)
                {
                    if (data.ProcessID != processId)
                    {
                        return;
                    }

                    dumpState.EventPipeDataPresent = true;

                    if (gcNum < 0 && data.Depth == 2 && data.Type != GCType.BackgroundGC)
                    {
                        gcNum = data.Count;
                        log.WriteLine("{0,5:n1}s: .NET Dump Started...", getElapsed().TotalSeconds);
                    }
                };

                gcDumpSession.Source.Clr.GCStop += delegate (GCEndTraceData data)
                {
                    if (data.ProcessID != processId)
                    {
                        return;
                    }

                    if (data.Count == gcNum)
                    {
                        log.WriteLine("{0,5:n1}s: .NET GC Complete.", getElapsed().TotalSeconds);
                        dumpState.DumpComplete = true;
                    }
                };

                gcDumpSession.Source.Clr.GCBulkNode += delegate (GCBulkNodeTraceData data)
                {
                    if (data.ProcessID != processId)
                    {
                        return;
                    }

                    dumpState.EventPipeDataPresent = true;

                    if ((getElapsed() - lastEventPipeUpdate).TotalMilliseconds > 500)
                    {
                        log.WriteLine("{0,5:n1}s: Making GC Heap Progress...", getElapsed().TotalSeconds);
                    }

                    lastEventPipeUpdate = getElapsed();
                };

                if (memoryGraph != null)
                {
                    dumper.SetupCallbacks(memoryGraph, gcDumpSession.Source, processId.ToString());
                }

                // Set up a separate thread that will listen for EventPipe events coming back telling us we succeeded.
                Task readerTask = Task.Run(() => {
                    // cancelled before we began
                    if (ct.IsCancellationRequested)
                    {
                        return;
                    }

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

                    if (!dumpState.EventPipeDataPresent && getElapsed().TotalSeconds > 5)      // Assume it started within 5 seconds.
                    {
                        log.WriteLine("{0,5:n1}s: Assume no .NET Heap", getElapsed().TotalSeconds);
                        break;
                    }

                    if (getElapsed().TotalSeconds > timeout)       // Time out after `timeout` seconds. defaults to 30s.
                    {
                        log.WriteLine("{0,5:n1}s: Timed out after {1} seconds", getElapsed().TotalSeconds, timeout);
                        break;
                    }

                    if (dumpState.DumpComplete)
                    {
                        break;
                    }
                }

                Task stopTask = Task.Run(() => {
                    log.WriteLine("{0,5:n1}s: Shutting down gcdump EventPipe session", getElapsed().TotalSeconds);
                    gcDumpSession.EndSession();
                    log.WriteLine("{0,5:n1}s: gcdump EventPipe session shut down", getElapsed().TotalSeconds);
                }, ct);

                try
                {
                    while (!Task.WaitAll(new Task[] { readerTask, stopTask }, 1000))
                    {
                        log.WriteLine("{0,5:n1}s: still reading...", getElapsed().TotalSeconds);
                    }
                }
                catch (AggregateException ae) // no need to throw if we're just cancelling the tasks
                {
                    foreach (Exception e in ae.Flatten().InnerExceptions)
                    {
                        if (e is not TaskCanceledException)
                        {
                            throw;
                        }
                    }
                }

                log.WriteLine("{0,5:n1}s: gcdump EventPipe Session closed", getElapsed().TotalSeconds);

                if (ct.IsCancellationRequested)
                {
                    return false;
                }

                if (dumpState.EventPipeDataPresent)
                {
                    dumper.ConvertHeapDataToGraph();        // Finish the conversion.
                    success = dumpState.DumpComplete;
                }
            }
            catch (Exception e)
            {
                log.WriteLine($"{getElapsed().TotalSeconds,5:n1}s: [Error] Exception during gcdump: {e}");
            }

            log.WriteLine("[{0,5:n1}s: Done Dumping .NET heap success={1}]", getElapsed().TotalSeconds, success);

            return success;
        }

        private sealed class DumpState
        {
            public volatile bool EventPipeDataPresent;
            public volatile bool DumpComplete;
        }
    }

    internal sealed class EventPipeSessionController : IDisposable
    {
        // mirrors EventPipeSession.DefaultRundownKeyword which is internal in Microsoft.Diagnostics.NETCore.Client
        private const long DefaultRundownKeyword = 0x80020139;

        private List<EventPipeProvider> _providers;
        private DiagnosticsClient _client;
        private EventPipeSession _session;
        private EventPipeEventSource _source;
        private int _pid;

        public IReadOnlyList<EventPipeProvider> Providers => _providers.AsReadOnly();
        public EventPipeEventSource Source => _source;

        public EventPipeSessionController(int pid, List<EventPipeProvider> providers, bool requestRundown = true, EventPipeBufferingMode bufferingMode = EventPipeBufferingMode.Drop)
        {
            _pid = pid;
            _providers = providers;
            _client = new DiagnosticsClient(pid);

            EventPipeSessionConfiguration sessionConfig = new(providers, 1024, rundownKeyword: requestRundown ? DefaultRundownKeyword : 0, requestStackwalk: true, bufferingMode: bufferingMode);
            try
            {
                _session = _client.StartEventPipeSession(sessionConfig);
            }
            catch (UnsupportedCommandException) when (bufferingMode != EventPipeBufferingMode.Drop)
            {
                // The runtime doesn't recognize the CollectTracing v6 command used for non-lossy (Block)
                // buffering (introduced in .NET 11). Fall back to the default lossy buffering so a gcdump can
                // still be collected. A runtime that understands the command but rejects the payload responds
                // with BadEncoding (a BadEncodingException, not an UnsupportedCommandException), which is not
                // caught here and so surfaces normally.
                Console.Error.WriteLine("Warning: the target runtime does not support non-lossy (Block) buffering, which requires .NET 11+. Falling back to the default lossy buffering; the gcdump may be incomplete on large heaps.");
                EventPipeSessionConfiguration fallbackConfig = new(providers, 1024, rundownKeyword: requestRundown ? DefaultRundownKeyword : 0, requestStackwalk: true, bufferingMode: EventPipeBufferingMode.Drop);
                _session = _client.StartEventPipeSession(fallbackConfig);
            }
            _source = new EventPipeEventSource(_session.EventStream);
        }

        public void EndSession()
        {
            _session.Stop();
        }

        #region IDisposable Support
        private bool disposedValue; // To detect redundant calls

        private void Dispose(bool disposing)
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
