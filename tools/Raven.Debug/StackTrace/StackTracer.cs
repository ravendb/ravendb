using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Diagnostics.NETCore.Client;
using Microsoft.Diagnostics.Symbols;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Stacks;
using Newtonsoft.Json;

namespace Raven.Debug.StackTrace
{
    public static class StackTracer
    {
        public static async Task ShowStackTrace(int processId, uint attachTimeout, string outputPath, TextWriter writer, HashSet<int> threadIds)
        {
            string tempNetTraceFilename = Path.Join(Path.GetTempPath(), Path.GetRandomFileName() + ".nettrace");
            string tempEtlxFilename = "";

            try
            {
                if (processId <= 0)
                {
                    throw new InvalidOperationException("Uninitialized process id parameter");
                }

                var client = new DiagnosticsClient(processId);
                var providers = new List<EventPipeProvider>()
                {
                    new EventPipeProvider("Microsoft-DotNETCore-SampleProfiler", EventLevel.Informational)
                };

                // collect a *short* trace with stack samples
                // the hidden '--duration' flag can increase the time of this trace in case 10ms
                // is too short in a given environment, e.g., resource constrained systems
                // N.B. - This trace INCLUDES rundown.  For sufficiently large applications, it may take non-trivial time to collect
                //        the symbol data in rundown.
                using (EventPipeSession session = client.StartEventPipeSession(providers))
                await using (FileStream fs = File.OpenWrite(tempNetTraceFilename))
                {
                    Task copyTask = session.EventStream.CopyToAsync(fs);
                    await Task.Delay(TimeSpan.FromMilliseconds(attachTimeout));
                    session.Stop();

                    await copyTask;
                }

                // using the generated trace file, symbolicate and compute stacks.
                tempEtlxFilename = TraceLog.CreateFromEventPipeDataFile(tempNetTraceFilename);
                using (var symbolReader = new SymbolReader(System.IO.TextWriter.Null) { SymbolPath = SymbolPath.MicrosoftSymbolServerPath })
                using (var eventLog = new TraceLog(tempEtlxFilename))
                {
                    var stackSource = new MutableTraceEventStackSource(eventLog)
                    {
                        OnlyManagedCodeStacks = true
                    };

                    var computer = new SampleProfilerThreadTimeComputer(eventLog, symbolReader);
                    computer.GenerateThreadTimeStacks(stackSource);

                    var samplesForThread = new Dictionary<int, List<StackSourceSample>>();

                    stackSource.ForEach((sample) =>
                    {
                        var stackIndex = sample.StackIndex;
                        while (!stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), false).StartsWith("Thread ("))
                            stackIndex = stackSource.GetCallerIndex(stackIndex);

                        // long form for: int.Parse(threadFrame["Thread (".Length..^1)])
                        // Thread id is in the frame name as "Thread (<ID>)"
                        const string template = "Thread (";
                        string threadFrame = stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), false);

                        // we are looking for the first index of ) because
                        // we need to handle a thread name like this: Thread (4008) (.NET IO ThreadPool Worker)
                        var firstIndex = threadFrame.IndexOf(")"); 
                        var threadId = int.Parse(threadFrame.Substring(template.Length, firstIndex - template.Length));

                        if (samplesForThread.TryGetValue(threadId, out var samples))
                        {
                            samples.Add(sample);
                        }
                        else
                        {
                            samplesForThread[threadId] = new List<StackSourceSample>() { sample };
                        }
                    });

                    var mergedStackTraces = MergeStackTraces(samplesForThread, stackSource, threadIds);

                    using (GetOutputWriter(outputPath, writer, out var outputWriter))
                        OutputResult(outputWriter, mergedStackTraces);
                }
            }
            finally
            {
                if (File.Exists(tempNetTraceFilename))
                    File.Delete(tempNetTraceFilename);
                if (File.Exists(tempEtlxFilename))
                    File.Delete(tempEtlxFilename);
            }
        }

        private static List<StackInfo> MergeStackTraces(Dictionary<int, List<StackSourceSample>> samplesForThread, StackSource stackSource, HashSet<int> threadIds)
        {
            var mergedStackTraces = new List<StackInfo>();

            foreach (var sampleForThread in samplesForThread)
            {
                var threadId = sampleForThread.Key;

                if (threadIds?.Count > 0 && threadIds.Contains(threadId) == false)
                    continue;

                var merged = false;
                var stackTrace = GetStackTrace(sampleForThread.Value[0], stackSource);

                foreach (var mergedStack in mergedStackTraces)
                {
                    if (stackTrace.SequenceEqual(mergedStack.StackTrace, StringComparer.OrdinalIgnoreCase) == false)
                        continue;

                    if (mergedStack.ThreadIds.Contains(threadId) == false)
                        mergedStack.ThreadIds.Add(threadId);

                    merged = true;
                    break;
                }

                if (merged)
                    continue;

                mergedStackTraces.Add(new StackInfo
                {
                    ThreadIds = new List<int>
                    {
                        sampleForThread.Key
                    },
                    StackTrace = stackTrace,
                });
            }

            return mergedStackTraces;
        }

        private static List<string> GetStackTrace(StackSourceSample stackSourceSample, StackSource stackSource)
        {
            var result = new List<string>();

            var stackIndex = stackSourceSample.StackIndex;
            while (!stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), verboseName: false).StartsWith("Thread ("))
            {
                var frame = $"{stackSource.GetFrameName(stackSource.GetFrameIndex(stackIndex), verboseName: false)}"
                    .Replace("UNMANAGED_CODE_TIME", "[Native Frames]");

                result.Add(frame);

                stackIndex = stackSource.GetCallerIndex(stackIndex);
            }

            return result;
        }

        private static IDisposable GetOutputWriter(string outputPath, TextWriter writer, out TextWriter outputWriter)
        {
            if (outputPath != null)
            {
                var output = File.Create(outputPath);
                outputWriter = new StreamWriter(output);
                return outputWriter;
            }

            outputWriter = writer;
            return null;
        }

        private static void OutputResult(TextWriter outputWriter, object results)
        {
            var jsonSerializer = new JsonSerializer
            {
                Formatting = Formatting.Indented
            };

            var result = new
            {
                Results = results
            };

            jsonSerializer.Serialize(outputWriter, result);

            outputWriter.Flush();
        }
    }
}
