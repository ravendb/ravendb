using System;
using Microsoft.Diagnostics.Runtime;
using rvn.Utils;

namespace rvn
{
    internal class ClrMd : IDisposable
    {
        private readonly int? _tid;
        private readonly string _command;
        private readonly DataTarget _dataTarget;
        private readonly ClrInfo _clrVersion;
        private readonly string _dac;

        public ClrMd(int? pid, int? tid, string command)
        {
            pid = pid ?? ServerProcessUtil.GetRavenServerPid();
            _tid = tid;
            _command = command;

            _dataTarget = DataTarget.AttachToProcess(pid.Value, 1000, AttachFlag.NonInvasive);
            _clrVersion = _dataTarget.ClrVersions[0];
            _dac = _dataTarget.SymbolLocator.FindBinary(_clrVersion.DacInfo);
        }

        public void CollectData()
        {
            var runtime = _clrVersion.CreateRuntime(_dac);

            foreach (var clrThread in runtime.Threads)
            {
                if (clrThread.IsAlive == false)
                    continue;

                if (_tid != null && _tid != clrThread.OSThreadId)
                    continue;

                Console.WriteLine("Thread {0:X}:", clrThread.OSThreadId);
                Console.WriteLine("Stack: {0:X} - {1:X}", clrThread.StackBase, clrThread.StackLimit);

                if (_command.Equals("!ClrStack", StringComparison.OrdinalIgnoreCase))
                {
                    // Walk the stack of the thread and print output similar to !ClrStack.
                    Console.WriteLine();
                    Console.WriteLine("Managed Callstack:");

                    foreach (var frame in clrThread.StackTrace)
                    {
                        // Note that CLRStackFrame currently only has three pieces of data: stack pointer,
                        // instruction pointer, and frame name (which comes from ToString).  Future
                        // versions of this API will allow you to get the type/function/module of the
                        // method (instead of just the name).  This is not yet implemented.
                        Console.WriteLine("{0,16:X} {1,16:X} {2}", frame.StackPointer, frame.InstructionPointer, frame.DisplayString);
                    }
                }
                else if (_command.Equals("!DumpStackObjects", StringComparison.OrdinalIgnoreCase))
                {
                    // We'll need heap data to find objects on the stack.
                    var heap = runtime.Heap;

                    // Walk each pointer aligned address on the stack.  Note that StackBase/StackLimit
                    // is exactly what they are in the TEB.  This means StackBase > StackLimit on AMD64.
                    var start = clrThread.StackBase;
                    var stop = clrThread.StackLimit;

                    // We'll walk these in pointer order.
                    if (start > stop)
                    {
                        var tmp = start;
                        start = stop;
                        stop = tmp;
                    }

                    Console.WriteLine();
                    Console.WriteLine("Stack objects:");

                    // Walk each pointer aligned address.  Ptr is a stack address.
                    for (var ptr = start; ptr <= stop; ptr += (ulong)runtime.PointerSize)
                    {
                        // Read the value of this pointer.  If we fail to read the memory, break.  The
                        // stack region should be in the crash dump.
                        if (runtime.ReadPointer(ptr, out var obj) == false)
                            break;

                        // 003DF2A4 
                        // We check to see if this address is a valid object by simply calling
                        // GetObjectType.  If that returns null, it's not an object.
                        var type = heap.GetObjectType(obj);
                        if (type == null)
                            continue;

                        // Don't print out free objects as there tends to be a lot of them on
                        // the stack.
                        if (type.IsFree == false)
                            Console.WriteLine("{0,16:X} {1,16:X} {2}", ptr, obj, type.Name);
                    }
                }
            }

        }

        public void Dispose()
        {
            _dataTarget?.Dispose();
        }
    }
}
