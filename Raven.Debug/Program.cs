using System.IO;
using Newtonsoft.Json;

namespace Raven.Debug
{
	using Microsoft.Diagnostics.Runtime;
	using Microsoft.Diagnostics.Runtime.Interop;
	using NDesk.Options;
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Text;

	class Program
	{
		static void Main(string[] args)
		{
			int processId = -1;
			uint attachTimeout = 15000;
			Action actionToTake = null;
			string outputFilePath = null;

			var optionSet = new OptionSet
			{
				{"pid=", "Process id.", pid => processId = int.Parse(pid)},
				{"attachTimeout=", "Attaching to process timeout in miliseconds. Default 15000.", timeout => attachTimeout = uint.Parse(timeout)},
				{"output=", "Output file path.", path => outputFilePath = path},
				{"stacktrace", "Print stacktraces of the attached process.", x => actionToTake = () => ShowStackTrace(processId, attachTimeout, outputFilePath)}
			};

			try
			{
				if (args.Length == 0)
				{
					PrintUsage(optionSet);
					return;
				}

				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine(e.Message);
				PrintUsage(optionSet);
				return;
			}

			if (actionToTake != null)
				actionToTake();
		}

		private static void ShowStackTrace(int processId, uint attachTimeout, string outputPath)
		{
			if (processId == -1)
				throw new InvalidOperationException("Uinitialized process id parameter");

			var threadInfoList = new List<ThreadInfo>();

			using (DataTarget dataTarget = DataTarget.AttachToProcess(processId, attachTimeout))
			{
				var dacLocation = dataTarget.ClrVersions[0].TryGetDacLocation();
				var runtime = dataTarget.CreateRuntime(dacLocation);
				var control = (IDebugControl)dataTarget.DebuggerInterface;
				var sysObjs = (IDebugSystemObjects)dataTarget.DebuggerInterface;
				var nativeFrames = new DEBUG_STACK_FRAME[100];
				var sybSymbols = (IDebugSymbols)dataTarget.DebuggerInterface;

				var sb = new StringBuilder(1024 * 1024);

				foreach (ClrThread thread in runtime.Threads)
				{

					var threadInfo = new ThreadInfo
					{
						OSThreadId = thread.OSThreadId
					};

					if (thread.StackTrace.Count > 0)
					{
						foreach (ClrStackFrame frame in thread.StackTrace)
						{
							if (frame.DisplayString.Equals("GCFrame") || frame.DisplayString.Equals("DebuggerU2MCatchHandlerFrame"))
								continue;

							threadInfo.StackTrace.Add(frame.DisplayString);
						}
					}
					else
					{
						threadInfo.IsNative = true;

						sysObjs.SetCurrentThreadId(threadInfo.OSThreadId);

						uint frameCount;
						control.GetStackTrace(0, 0, 0, nativeFrames, 100, out frameCount);

						for (int i = 0; i < frameCount; i++)
						{
							uint nameSize;
							ulong dis;

							sb.Clear();
							sybSymbols.GetNameByOffset(nativeFrames[i].InstructionOffset, sb, sb.Capacity, out nameSize, out dis);

							threadInfo.StackTrace.Add(sb.ToString());
						}
					}

					threadInfoList.Add(threadInfo);
				}
			}

			var mergedStackTraces = new List<StackInfo>();

			foreach (var threadInfo in threadInfoList)
			{
				bool merged = false;

				foreach (var mergedStack in mergedStackTraces)
				{
					if (threadInfo.IsNative != mergedStack.NativeThreads)
						continue;

					if (threadInfo.StackTrace.SequenceEqual(mergedStack.StackTrace, StringComparer.InvariantCultureIgnoreCase) == false)
						continue;

					if (mergedStack.ThreadIds.Contains(threadInfo.OSThreadId) == false)
						mergedStack.ThreadIds.Add(threadInfo.OSThreadId);

					merged = true;
					break;
				}

				if (merged)
					continue;

				mergedStackTraces.Add(new StackInfo()
				{
					ThreadIds = new List<uint>() { threadInfo.OSThreadId },
					StackTrace = threadInfo.StackTrace,
					NativeThreads = threadInfo.IsNative
				});
			}

			var jsonSerializer = new JsonSerializer
			{
				Formatting = Formatting.Indented
			};

			if (outputPath != null)
			{
				using (var output = File.Create(outputPath))
				using (var streamWriter = new StreamWriter(output))
				{
					jsonSerializer.Serialize(streamWriter, mergedStackTraces);
				}
			}
			else
			{
				jsonSerializer.Serialize(Console.Out, mergedStackTraces);
			}
		}

		private static void PrintUsage(OptionSet optionSet)
		{
			Console.WriteLine(
				@"
RavenDB
Document Database for the .Net Platform
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:",
				DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);

			Console.WriteLine(@"
Enjoy...
");
		}
	}
}
