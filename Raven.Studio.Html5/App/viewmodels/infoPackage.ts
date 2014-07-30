import appUrl = require("common/appUrl");
import database = require("models/database");
import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require('d3/d3');
import nv = require('nvd3');

enum parserState {
  pid,
  stack
}

class stackInfo {
    threadIds: string[];
    stack: string[];
    children: stackInfo[];

    depth: number;

    static boxPadding = 8;
    static lineHeight = 12;
    static headerSize = 16;

    boxHeight = () => {
        return this.stack.length * stackInfo.lineHeight + 2 * stackInfo.boxPadding;
    }

    stackWithShortcuts = () => {
        return this.stack.map(v => {
            return {
                short: stackInfo.shortName(v),
                full: v
            }
        });
    }

    static shortName(v: string) {
        var withoutArgs = v.replace(/\(.*?\)/g, '');
        if (withoutArgs.contains('+')) {
            return withoutArgs.replace(/.*\.(.*\+.*)/, '$1');
        } else {
            return withoutArgs.replace(/.*\.([^\.]+\.[^\.]+)$/, '$1');
        }
        
    }

    static isUserCode(line: string): boolean {
        return line.startsWith("Raven") || line.startsWith("Voron");
    }

    constructor(threadIds: string[], stack: string[]) {
        this.threadIds = threadIds;
        this.stack = stack;
    }
}

class infoPackage extends viewModelBase {
    static tempText = "2014-07-23 11:22:58	MARCIN-WIN	pid='6384'\r\n" +
    "\r\n" +
    "OS Thread Id:7036\r\n" +
    "\r\n" +
    "OS Thread Id:7024\r\n" +
    "  Microsoft.VisualStudio.HostingProcess.HostProc.WaitForThreadExit()\r\n" +
    "  Microsoft.VisualStudio.HostingProcess.HostProc.RunParkingWindowThread(N/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/A)\r\n" +
    "\r\n" +
    "OS Thread Id:5912\r\n" +
    "  Microsoft.Win32.SystemEvents.WindowThreadProc(Microsoft.Win32.SystemEvents)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6644\r\n" +
    "  System.IO.__ConsoleStream.ReadFileNative(N/AN/AN/AN/AN/AN/AN/A)\r\n" +
    "  System.IO.__ConsoleStream.Read(N/AN/AN/AN/A)\r\n" +
    "  System.IO.StreamReader.ReadBuffer(N/A)\r\n" +
    "  System.IO.StreamReader.ReadLine(N/A)\r\n" +
    "  System.IO.TextReader+SyncTextReader.ReadLine(System.IO.TextReader+SyncTextReader)\r\n" +
    "  Raven.Server.Program.InteractiveRun(Raven.Server.RavenDbServer)\r\n" +
    "  Raven.Server.Program.RunServerInDebugMode(Raven.Database.Config.RavenConfigurationSystem.Boolean)\r\n" +
    "  Raven.Server.Program.RunInDebugMode(System.Nullable`1<Raven.Database.Server.AnonymousUserAccessMode>Raven.Database.Config.RavenConfigurationSystem.BooleanSystem.Boolean)\r\n" +
    "  Raven.Server.Program+<>c__DisplayClass34.<InteractiveRun>b__1b(Raven.Server.Program+<>c__DisplayClass34)\r\n" +
    "  Raven.Server.Program.InteractiveRun(System.String[])\r\n" +
    "  Raven.Server.Program.Main(System.String[])\r\n" +
    "  Microsoft.VisualStudio.HostingProcess.HostProc.RunUsersAssembly()\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/A)\r\n" +
    "\r\n" +
    "OS Thread Id:4072\r\n" +
    "  Raven.Database.Config.MemoryStatistics+<>c__DisplayClass4.<.cctor>b__2(Raven.Database.Config.MemoryStatistics+<>c__DisplayClass4)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/A)\r\n" +
    "\r\n" +
    "OS Thread Id:5608\r\n" +
    "\r\n" +
    "OS Thread Id:4076\r\n" +
    "\r\n" +
    "OS Thread Id:2164\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(N/AN/A)\r\n" +
    "  Voron.StorageEnvironment.<FlushWritesToDataFileAsync>b__7(Voron.StorageEnvironment)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6940\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  Voron.Impl.TransactionMergingWriter.BackgroundWriter(Voron.Impl.TransactionMergingWriter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:3236\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.IndexingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:5496\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.ReducingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:728\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(N/AN/A)\r\n" +
    "  Voron.StorageEnvironment.<FlushWritesToDataFileAsync>b__7(Voron.StorageEnvironment)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:5380\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  Voron.Impl.TransactionMergingWriter.BackgroundWriter(Voron.Impl.TransactionMergingWriter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6188\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.IndexingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6212\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.ReducingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:2212\r\n" +
    "\r\n" +
    "OS Thread Id:5060\r\n" +
    "\r\n" +
    "OS Thread Id:6800\r\n" +
    "\r\n" +
    "OS Thread Id:4968\r\n" +
    "\r\n" +
    "OS Thread Id:6712\r\n" +
    "\r\n" +
    "OS Thread Id:6760\r\n" +
    "\r\n" +
    "OS Thread Id:4236\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(N/AN/A)\r\n" +
    "  Voron.StorageEnvironment.<FlushWritesToDataFileAsync>b__7(Voron.StorageEnvironment)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:3012\r\n" +
    "  System.Threading.ManualResetEventSlim.Wait(System.Threading.ManualResetEventSlimSystem.Int32N/A)\r\n" +
    "  Voron.Impl.TransactionMergingWriter.BackgroundWriter(Voron.Impl.TransactionMergingWriter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:3452\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.String)\r\n" +
    "  Raven.Bundles.Replication.Tasks.ReplicationTask.Execute(Raven.Bundles.Replication.Tasks.ReplicationTask)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6180\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.IndexingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:6728\r\n" +
    "  System.Threading.Monitor.Wait(N/AN/A)\r\n" +
    "  Raven.Database.Indexing.WorkContext.WaitForWork(Raven.Database.Indexing.WorkContextSystem.TimeSpanSystem.Int32&System.ActionSystem.String)\r\n" +
    "  Raven.Database.Indexing.AbstractIndexingExecuter.Execute(Raven.Database.Indexing.ReducingExecuter)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.Task)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.TaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/A)\r\n" +
    "  System.Threading.ThreadHelper.ThreadStart(N/AN/A)\r\n" +
    "\r\n" +
    "OS Thread Id:5112\r\n" +
    "\r\n" +
    "OS Thread Id:6668\r\n" +
    "\r\n" +
    "OS Thread Id:5016\r\n" +
    "  System.Threading.WaitHandle.InternalWaitOne(N/AN/AN/AN/A)\r\n" +
    "  System.Diagnostics.Process.WaitForExit(System.Diagnostics.ProcessSystem.Int32)\r\n" +
    "  Raven.Database.Server.Controllers.DebugController.InfoPackage(Raven.Database.Server.Controllers.DebugController)\r\n" +
    "  System.Web.Http.Controllers.ReflectedHttpActionDescriptor+ActionExecutor+<>c__DisplayClass10.<GetExecutor>b__9(System.Web.Http.Controllers.ReflectedHttpActionDescriptor+ActionExecutor+<>c__DisplayClass10Raven.Database.Server.Controllers.DebugControllerSystem.Object[])\r\n" +
    "  System.Web.Http.Controllers.ReflectedHttpActionDescriptor+ActionExecutor.Execute(System.Web.Http.Controllers.ReflectedHttpActionDescriptor+ActionExecutorRaven.Database.Server.Controllers.DebugControllerSystem.Object[])\r\n" +
    "  System.Web.Http.Controllers.ReflectedHttpActionDescriptor.ExecuteAsync(System.Web.Http.Controllers.ReflectedHttpActionDescriptorSystem.Web.Http.Controllers.HttpControllerContextSystem.Collections.Generic.Dictionary`2<System.String,System.Object>System.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Controllers.ApiControllerActionInvoker+<InvokeActionAsyncCore>d__0.MoveNext(System.Web.Http.Controllers.ApiControllerActionInvoker+<InvokeActionAsyncCore>d__0&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.Controllers.ApiControllerActionInvoker.InvokeActionAsyncCore(System.Web.Http.Controllers.HttpActionContextSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Controllers.ApiControllerActionInvoker.InvokeActionAsync(System.Web.Http.Controllers.ApiControllerActionInvokerSystem.Web.Http.Controllers.HttpActionContextSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Controllers.ActionFilterResult+ActionInvoker.InvokeActionAsync(System.Web.Http.Controllers.ActionFilterResult+ActionInvoker&)\r\n" +
    "  System.Web.Http.Controllers.ActionFilterResult+<ExecuteAsync>d__2.MoveNext(System.Web.Http.Controllers.ActionFilterResult+<ExecuteAsync>d__2&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.Controllers.ActionFilterResult.ExecuteAsync(System.Web.Http.Controllers.ActionFilterResultSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Controllers.ExceptionFilterResult+<ExecuteAsync>d__0.MoveNext(System.Web.Http.Controllers.ExceptionFilterResult+<ExecuteAsync>d__0&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.Controllers.ExceptionFilterResult.ExecuteAsync(System.Web.Http.Controllers.ExceptionFilterResultSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.ApiController.ExecuteAsync(Raven.Database.Server.Controllers.DebugControllerSystem.Web.Http.Controllers.HttpControllerContextSystem.Threading.CancellationToken)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController.<>n__FabricatedMethod45(Raven.Database.Server.Controllers.DebugControllerSystem.Web.Http.Controllers.HttpControllerContextSystem.Threading.CancellationToken)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController+<ExecuteActualRequest>d__3e.MoveNext(Raven.Database.Server.Controllers.RavenDbApiController+<ExecuteActualRequest>d__3e&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController.ExecuteActualRequest(Raven.Database.Server.Controllers.DebugControllerSystem.Web.Http.Controllers.HttpControllerContextSystem.Threading.CancellationTokenRaven.Database.Server.Security.MixedModeRequestAuthorizer)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController+<>c__DisplayClass35+<<ExecuteAsync>b__31>d__37.MoveNext(Raven.Database.Server.Controllers.RavenDbApiController+<>c__DisplayClass35+<<ExecuteAsync>b__31>d__37&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController+<>c__DisplayClass35.<ExecuteAsync>b__31(Raven.Database.Server.Controllers.RavenDbApiController+<>c__DisplayClass35)\r\n" +
    "  Raven.Database.Server.WebApi.RequestManager+<HandleActualRequest>d__1.MoveNext(Raven.Database.Server.WebApi.RequestManager+<HandleActualRequest>d__1&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Raven.Database.Server.WebApi.RequestManager.HandleActualRequest(Raven.Database.Server.WebApi.RequestManagerRaven.Database.Server.Controllers.DebugControllerSystem.Func`1<System.Threading.Tasks.Task`1<System.Net.Http.HttpResponseMessage>>System.Func`2<System.Web.HttpException,System.Net.Http.HttpResponseMessage>)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController+<ExecuteAsync>d__3a.MoveNext(Raven.Database.Server.Controllers.RavenDbApiController+<ExecuteAsync>d__3a&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Raven.Database.Server.Controllers.RavenDbApiController.ExecuteAsync(Raven.Database.Server.Controllers.DebugControllerSystem.Web.Http.Controllers.HttpControllerContextSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Dispatcher.HttpControllerDispatcher.SendAsyncCore(System.Web.Http.Dispatcher.HttpControllerDispatcherSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.Dispatcher.HttpControllerDispatcher+<SendAsync>d__0.MoveNext(System.Web.Http.Dispatcher.HttpControllerDispatcher+<SendAsync>d__0&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.Dispatcher.HttpControllerDispatcher.SendAsync(System.Web.Http.Dispatcher.HttpControllerDispatcherSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  System.Net.Http.HttpMessageInvoker.SendAsync(N/AN/AN/A)\r\n" +
    "  System.Web.Http.Dispatcher.HttpRoutingDispatcher.SendAsync(System.Web.Http.Dispatcher.HttpRoutingDispatcherSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandler.<>n__FabricatedMethod9(Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandlerSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandler+<SendAsync>d__3.MoveNext(Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandler+<SendAsync>d__3&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandler.SendAsync(Raven.Database.Server.WebApi.Handlers.GZipToJsonAndCompressHandlerSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.HttpServer.<>n__FabricatedMethod9(System.Web.Http.HttpServerSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  System.Web.Http.HttpServer+<SendAsync>d__0.MoveNext(System.Web.Http.HttpServer+<SendAsync>d__0&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.HttpServer.SendAsync(System.Web.Http.HttpServerSystem.Net.Http.HttpRequestMessageSystem.Threading.CancellationToken)\r\n" +
    "  System.Net.Http.HttpMessageInvoker.SendAsync(N/AN/AN/A)\r\n" +
    "  System.Web.Http.Owin.HttpMessageHandlerAdapter+<InvokeCore>d__0.MoveNext(System.Web.Http.Owin.HttpMessageHandlerAdapter+<InvokeCore>d__0&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  System.Web.Http.Owin.HttpMessageHandlerAdapter.InvokeCore(System.Web.Http.Owin.HttpMessageHandlerAdapterMicrosoft.Owin.OwinContextMicrosoft.Owin.OwinRequestMicrosoft.Owin.OwinResponse)\r\n" +
    "  System.Web.Http.Owin.HttpMessageHandlerAdapter.Invoke(System.Web.Http.Owin.HttpMessageHandlerAdapterMicrosoft.Owin.OwinContext)\r\n" +
    "  Microsoft.Owin.Extensions.UseHandlerMiddleware+<>c__DisplayClass3+<>c__DisplayClass5.<.ctor>b__1(Microsoft.Owin.Extensions.UseHandlerMiddleware+<>c__DisplayClass3+<>c__DisplayClass5)\r\n" +
    "  Owin.AppBuilderExtensions+<UpgradeToWebSockets>d__3.MoveNext(Owin.AppBuilderExtensions+<UpgradeToWebSockets>d__3&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Owin.AppBuilderExtensions.UpgradeToWebSockets(Raven.Database.Server.RavenDBOptionsMicrosoft.Owin.OwinContextSystem.Func`1<System.Threading.Tasks.Task>)\r\n" +
    "  Owin.AppBuilderExtensions+<>c__DisplayClass1.<UseRavenDB>b__0(Owin.AppBuilderExtensions+<>c__DisplayClass1Microsoft.Owin.OwinContextSystem.Func`1<System.Threading.Tasks.Task>)\r\n" +
    "  Microsoft.Owin.Extensions.UseHandlerMiddleware+<>c__DisplayClass3.<.ctor>b__0(Microsoft.Owin.Extensions.UseHandlerMiddleware+<>c__DisplayClass3Microsoft.Owin.OwinContext)\r\n" +
    "  Microsoft.Owin.Extensions.UseHandlerMiddleware.Invoke(Microsoft.Owin.Extensions.UseHandlerMiddlewareMicrosoft.Owin.OwinContext)\r\n" +
    "  Owin.AppBuilderExtensions+InterceptMiddleware+<Invoke>d__9.MoveNext(Owin.AppBuilderExtensions+InterceptMiddleware+<Invoke>d__9&)\r\n" +
    "  System.Runtime.CompilerServices.AsyncMethodBuilderCore.Start(N/AN/A)\r\n" +
    "  Owin.AppBuilderExtensions+InterceptMiddleware.Invoke(Owin.AppBuilderExtensions+InterceptMiddlewareMicrosoft.Owin.OwinContext)\r\n" +
    "  Microsoft.Owin.Infrastructure.OwinMiddlewareTransition.Invoke(Microsoft.Owin.Infrastructure.OwinMiddlewareTransitionMicrosoft.Owin.Host.HttpListener.RequestProcessing.CallEnvironment)\r\n" +
    "  Microsoft.Owin.Hosting.Utilities.Encapsulate.Invoke(Microsoft.Owin.Hosting.Utilities.EncapsulateMicrosoft.Owin.Host.HttpListener.RequestProcessing.CallEnvironment)\r\n" +
    "  Microsoft.Owin.Host.HttpListener.OwinHttpListener.StartProcessingRequest(Microsoft.Owin.Host.HttpListener.OwinHttpListenerSystem.Net.HttpListenerContext)\r\n" +
    "  System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass36`1+<>c__DisplayClass38.<Then>b__35(System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass36`1+<>c__DisplayClass38<System.Net.HttpListenerContext>)\r\n" +
    "  System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass49.<ToAsyncVoidTask>b__48(System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass49)\r\n" +
    "  System.Threading.Tasks.TaskHelpers.RunSynchronously(System.Func`1<System.Threading.Tasks.Task`1<System.Threading.Tasks.TaskHelpersExtensions+AsyncVoid>>System.Threading.CancellationToken)\r\n" +
    "  System.Threading.Tasks.TaskHelpersExtensions.ToAsyncVoidTask(System.Action)\r\n" +
    "  System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass36`1.<Then>b__34(System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass36`1<System.Net.HttpListenerContext>System.Threading.Tasks.Task`1<System.Net.HttpListenerContext>)\r\n" +
    "  System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass45`2.<ThenImplContinuation>b__43(System.Threading.Tasks.TaskHelpersExtensions+<>c__DisplayClass45`2<System.Threading.Tasks.TaskHelpersExtensions+AsyncVoid,System.Threading.Tasks.Task`1<System.Net.HttpListenerContext>>System.Threading.Tasks.Task`1<System.Net.HttpListenerContext>)\r\n" +
    "  System.Threading.Tasks.Task.Execute(System.Threading.Tasks.ContinuationTaskFromTask)\r\n" +
    "  System.Threading.ExecutionContext.RunInternal(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.ExecutionContext.Run(N/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteWithThreadLocal(System.Threading.Tasks.ContinuationTaskFromTaskSystem.Threading.Tasks.Task&)\r\n" +
    "  System.Threading.Tasks.Task.ExecuteEntry(N/AN/A)\r\n" +
    "  System.Threading.Tasks.ThreadPoolTaskScheduler.TryExecuteTaskInline(System.Threading.Tasks.ThreadPoolTaskSchedulerN/ASystem.Boolean)\r\n" +
    "  System.Threading.Tasks.TaskScheduler.TryRunInline(N/ASystem.Threading.Tasks.ContinuationTaskFromTaskN/A)\r\n" +
    "  System.Threading.Tasks.TaskContinuation.InlineIfPossibleOrElseQueue(System.Threading.Tasks.ContinuationTaskFromTaskN/A)\r\n" +
    "  System.Threading.Tasks.Task.FinishContinuations(N/A)\r\n" +
    "  System.Threading.Tasks.Task`1.TrySetResult(N/AN/A)\r\n" +
    "  System.Threading.Tasks.TaskFactory`1.FromAsyncCoreLogic(N/AN/AN/AN/AN/A)\r\n" +
    "  System.Threading.Tasks.TaskFactory`1+<>c__DisplayClass9.<FromAsyncImpl>b__5(N/AN/A)\r\n" +
    "  System.Net.LazyAsyncResult.Complete(System.Net.ListenerAsyncResultN/A)\r\n" +
    "  System.Net.ListenerAsyncResult.IOCompleted(N/AN/AN/A)\r\n" +
    "  System.Threading._IOCompletionCallback.PerformIOCompletionCallback(System.Uint32System.Uint32System.Threading.NativeOverlapped*)\r\n";

    diagonal: any;

    node: D3.Selection = null; // nodes selection
    link: D3.Selection = null; // links selection

    private nodes: stackInfo[] = [];
    private links: D3.Layout.GraphLink[];

    private xScale: D3.Scale.LinearScale;
    private yScale: D3.Scale.LinearScale;

    private height: number;
    private width: number;

    static maxBoxWidth = 280;
    static boxVerticalPadding = 60;

    svg: D3.Selection = null;
    svgDefs: D3.Selection = null;
    graph: any = null;

    attached() {
        var stacksParsed = this.parseStackTraces(infoPackage.tempText);
        var collatedStacks = this.splitAndCollateStacks(stacksParsed);

        var self = this;
        this.diagonal = d3.svg.diagonal().projection(d => [self.xScale(d.x), self.yScale(d.y)]);

        this.svgDefs = d3.select("#parallelStacks").append("defs");

        this.svg = d3.select("#parallelStacks")
            .append("g")
            .call(d3.behavior.zoom().scaleExtent([0.2, 1.5]).on("zoom", this.zoom.bind(self)))
            .append("g");


        this.svg.append("rect")
            .attr("class", "overlay");
           
        this.node = this.svg.selectAll(".node");
        this.link = this.svg.selectAll(".link");

        this.updateGraph(collatedStacks);
    }

    private parseStackTraces(text: string): stackInfo[] {
        var tokens:string[] = text.split('\r\n');

        var header = tokens[0];

        // remove first two rows as it contains header and empty line
        tokens = tokens.splice(2);

        var line = 0;
        var state = parserState.pid;
        var currentPids: string[] = [];
        var stackStart = 0;

        var result: stackInfo[] = [];
        while (line < tokens.length) {
            if (state === parserState.pid) {
                if (tokens[line].startsWith("OS Thread Id")) {
                    currentPids.push(tokens[line].substr(12));
                }
                if (tokens[line + 1].trim()) {
                    // line not empty - so we started stack
                    state = parserState.stack;
                    stackStart = line;
                } else {
                    // skip empty line
                    line++;
                }
            } else if (state === parserState.stack) {
                if (line == tokens.length - 1) {
                    result.push(new stackInfo(currentPids, tokens.slice(stackStart + 1, line + 1).reverse().map(s => s.trim())));
                    currentPids = [];
                    state = parserState.pid;
                } else if (!tokens[line].trim()) {
                    result.push(new stackInfo(currentPids, tokens.slice(stackStart + 1, line).reverse().map(s => s.trim())));
                    currentPids = [];
                    state = parserState.pid;
                }
            }
            line++;
        }

        return result;
    }

    private splitAndCollateStacks(stacks: stackInfo[]): stackInfo[]{
        if (stacks.length == 1) {
            return stacks;
        }
        var grouped = d3.nest().key((d: stackInfo) => d.stack[0]).entries(stacks);

        // for each group find common stack
        return grouped.map(kv=> {
            var sharedStacks: stackInfo[] = kv.values;
            var minDepth = d3.min(sharedStacks, s => s.stack.length);

            outer:
            for (var depth = 0; depth < minDepth; depth++) {
                var currentStack = sharedStacks[0].stack[depth];
                for (var i = 1; i < sharedStacks.length; i++) {
                    if (currentStack != sharedStacks[i].stack[depth]) {
                        break outer;
                    }
                }
            }

            // extract shared stack:
            var sharedStack = new stackInfo([], sharedStacks[0].stack.slice(0, depth));

            // remove shared stack from all stacks and recurse
            var strippedStacks = sharedStacks.map(s => new stackInfo(s.threadIds, s.stack.slice(depth))).filter(s => s.stack.length > 0);
            sharedStack.children = this.splitAndCollateStacks(strippedStacks);
            sharedStack.threadIds = d3.merge(sharedStacks.map(s => s.threadIds));
            
            return sharedStack;
        });
    }

    private cumulativeSumWithPadding(input: any[], padding: number) {
        var currentSum = 0;
        var output = [0];
        for (var i = 0; i < input.length; i++) {
            var offset = padding + input[i];
            output.push(currentSum + offset);
            currentSum += offset;
        }
        return output;
    }

    getTooltip(data) {
        return data.full; 
    }

    private updateGraph(roots: stackInfo[]) {
        var self = this; 

        var fakeRoot: stackInfo = new stackInfo([], []);
        fakeRoot.children = roots;

        this.graph = d3.layout.tree().nodeSize([infoPackage.maxBoxWidth + 20, 100]);
        this.nodes = this.graph.nodes(fakeRoot).filter(d => d.depth > 0);

        var maxBoxHeightOnDepth = d3.nest()
            .key(d => d.depth)
            .sortKeys(d3.ascending)
            .rollup((leaves: any[]) => d3.max(leaves, l => l.boxHeight()))
            .entries(this.nodes)
            .map(v => v.values);

        var cumulative = this.cumulativeSumWithPadding(maxBoxHeightOnDepth, infoPackage.boxVerticalPadding);

        this.height = cumulative[cumulative.length - 1];
        var extent = d3.extent(this.nodes, (node: any) => node.x);
        extent[1] += infoPackage.maxBoxWidth; 
        this.width = extent[1] - extent[0];

        d3.select(".overlay")
            .attr("width", self.width)
            .attr("height", self.height);

        var halfBoxShift = infoPackage.maxBoxWidth / 2 + 10; // little padding

        this.xScale = d3.scale.linear().domain([extent[0] - halfBoxShift, extent[1] - halfBoxShift]).range([0, this.width]);
        this.yScale = d3.scale.linear().domain([0, this.height]).range([this.height, 0]);

        var yDepthScale = d3.scale.linear().domain(d3.range(1, cumulative.length + 2, 1)).range(cumulative);

        this.links = this.graph.links(this.nodes).map(link => {
            var targetY = yDepthScale(link.target.depth);
            var linkHeight = infoPackage.boxVerticalPadding - stackInfo.headerSize;

            return {
                source: {
                    x: link.source.x,
                    y: targetY - linkHeight,
                    y0: link.source.y
                },
                target: {
                    x: link.target.x,
                    y: targetY,
                    y0: link.target.y
                }
            }
        });

        this.node = this.node.data(this.nodes);
        this.link = this.link.data(this.links);

        var enteringNodes = (<D3.UpdateSelection>this.node)
            .enter()
            .append("g") 
            .attr("transform", (d) => "translate(" + this.xScale(d.x) + "," + this.yScale(yDepthScale(d.depth)) + ")");

        enteringNodes
            .filter((d: stackInfo) => d.children && d.children.length > 0)
            .append("line")
            .attr("class", "link")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", (d: stackInfo) => -d.boxHeight() - stackInfo.headerSize)
            .attr("y2", (d: stackInfo) => -maxBoxHeightOnDepth[d.depth - 1] - stackInfo.headerSize);

        var rect = enteringNodes.append('rect')
            .attr('class', 'box')
            .attr('x', -infoPackage.maxBoxWidth / 2)
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('width', infoPackage.maxBoxWidth)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize)
            .attr("fill", "red")
            .attr("rx", 5)
            .on('mouseout', () => nv.tooltip.cleanup());

        var clipPaths = this.svgDefs.selectAll('.stackClip').data(this.nodes);
        clipPaths
            .enter()
            .append("clipPath")
            .attr('class', 'stackClip')
            .attr('id', (d, i) => 'stack-clip-path-' + i)
            .append('rect')
            .attr('x', -infoPackage.maxBoxWidth / 2)
            .attr('width', infoPackage.maxBoxWidth - 5) // we substract little padding
            .attr('y', d => -1 * d.boxHeight() - stackInfo.headerSize)
            .attr('height', d => d.boxHeight() + stackInfo.headerSize);
                
        enteringNodes
            .append("text")
            .attr('text-anchor', 'middle')
            .attr('y', d => -1 * d.boxHeight())
            .text((d: stackInfo) => d.threadIds.length + " thread" + ((d.threadIds.length > 1) ? "s":''));

        enteringNodes
            .append("line")
            .attr('class', 'headerLine')
            .attr('x1', -1 * infoPackage.maxBoxWidth / 2)
            .attr('x2', infoPackage.maxBoxWidth / 2)
            .attr('y1', d => -1 * d.boxHeight() + 4)
            .attr('y2', d => -1 * d.boxHeight() + 4);
            
        enteringNodes.filter(d => d.depth > 0).each(function (d: stackInfo, index: number) {
            var g = this;
            var offsetTop = d.boxHeight() - stackInfo.boxPadding - stackInfo.lineHeight;
            var textGroup = d3.select(g)
                .append("g")
                .attr('class', 'traces')
                .style('clip-path', d => 'url(#stack-clip-path-' + index + ')');
            var stack = textGroup.selectAll('.trace').data(d.stackWithShortcuts().reverse());
            var reversedOriginalStack = d.stack.reverse();
            stack
                .enter()
                .append('text')
                .attr('x', -140 + stackInfo.boxPadding)
                .attr('y', (d, i) => -offsetTop + stackInfo.lineHeight * i)
                .text(d => d.short)
                .classed('notUserCode', (s, i) => !stackInfo.isUserCode(reversedOriginalStack[i]))
                .on('mouseover', function(d) {
                    nv.tooltip.cleanup();
                    var offset = $(this).offset(); 
                    nv.tooltip.show([offset.left, offset.top], self.getTooltip(d), 'n', 25);
                });
        });

        var enteringLinks = (<any>this.link)
            .enter()
            .append("g");

        enteringLinks
            .append("path")
            .attr("class", "link")
            .attr("d", this.diagonal);
    }

    zoom() {
        this.svg.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
    }
}

export = infoPackage;