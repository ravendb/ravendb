using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.ServiceProcess;
using System.Text.RegularExpressions;
using System.Threading;
using Sparrow.Utils;

// the windows-service command is registered only on Windows
#pragma warning disable CA1416 // Validate platform compatibility

namespace rvn
{
    public static partial class WindowsService
    {
        public static bool Register(string serviceName, string username, string password, string ravenServerDir, List<string> args)
        {
            return new WindowsServiceController(serviceName, username, password).Install(ravenServerDir, args);
        }

        public static bool Unregister(string serviceName)
        {
            return new WindowsServiceController(serviceName, username: null, password: null).Uninstall();
        }

        public static bool Start(string serviceName)
        {
            return new WindowsServiceController(serviceName, username: null, password: null).Start();
        }

        public static bool Stop(string serviceName)
        {
            return new WindowsServiceController(serviceName, username: null, password: null).Stop();
        }

        internal partial class WindowsServiceController
        {
            public const string WindowServiceDescription = "Next generation NoSQL Database";

            private const uint ErrorServiceExists = 0x00000431;

            private const uint ErrorServiceMarkedForDeletion = 0x00000430;

            private const uint ErrorAccessIsDenied = 0x00000005;

            private const string LocalServiceAccountName = @"NT AUTHORITY\LocalService";

            // the server reports its progress while stopping, so it is given enough time to shut down gracefully when the system shuts down
            // (the SCM continues the shutdown as soon as the service is stopped, the Windows default is only 10 seconds)
            private static readonly TimeSpan PreshutdownTimeout = TimeSpan.FromMinutes(3);

            private static readonly TimeSpan RecoveryResetPeriod = TimeSpan.FromDays(1);

            private static readonly TimeSpan RecoveryRestartDelay = TimeSpan.FromMinutes(1);

            private static readonly TimeSpan StatusChangeTimeout = TimeSpan.FromSeconds(60);

            private static readonly string ServiceControlToolPath = Path.Combine(Environment.SystemDirectory, "sc.exe");

            private readonly string _serviceName;
            private readonly string _username;
            private readonly string _password;

            private string ServiceFullName => $@"""{_serviceName} ({WindowServiceDescription})""";

            public WindowsServiceController(string serviceName, string username, string password)
            {
                _serviceName = serviceName;
                _username = username;
                _password = password;
            }

            public bool Install(string ravenServerDir, List<string> args)
            {
                string serviceCommand;
                try
                {
                    serviceCommand = GetServiceCommand(ravenServerDir, args);
                }
                catch (ArgumentException e)
                {
                    Console.WriteLine($"Cannot register service {ServiceFullName}: {e.Message}");
                    return false;
                }

                return InstallInternal(serviceCommand, reinstalling: false);
            }

            private bool InstallInternal(string serviceCommand, bool reinstalling, int counter = 0)
            {
                var normalizedServiceName = NormalizeServiceName(_serviceName);

                var createArgs = new List<string> { "create", normalizedServiceName };

                // sc.exe expects each option as two tokens: a name ending in '=' followed by the
                // value (e.g. "type=" then "own"), never "type=own".
                void AddOption(string name, string value)
                {
                    createArgs.Add(name + "=");
                    createArgs.Add(value);
                }

                AddOption("type", "own");
                AddOption("start", "auto");
                AddOption("error", "normal");
                AddOption("binPath", serviceCommand);
                AddOption("DisplayName", _serviceName);
                AddOption("obj", string.IsNullOrWhiteSpace(_username) ? LocalServiceAccountName : _username);
                if (string.IsNullOrWhiteSpace(_username) == false)
                    AddOption("password", _password ?? string.Empty);

                var result = RunServiceControlTool(createArgs);

                switch ((uint)result.ExitCode)
                {
                    case 0:
                        Configure(normalizedServiceName);

                        Console.WriteLine($"Service {ServiceFullName} has been registered.");
                        return true;

                    case ErrorServiceExists when reinstalling == false:
                        using (var serviceController = GetServiceController())
                        {
                            if (serviceController != null)
                            {
                                if (IsRavenServerService(serviceController) == false)
                                    return false;

                                Console.WriteLine($"Service {ServiceFullName} already exists. Reinstalling...");

                                if (UninstallInternal(serviceController) == false)
                                    return false;
                            }
                        }

                        return InstallInternal(serviceCommand, reinstalling: true);

                    case ErrorServiceMarkedForDeletion:
                        if (counter < 10)
                        {
                            Console.WriteLine($"Service {ServiceFullName} has been marked for deletion. Performing {counter + 1} installation attempt.");

                            Thread.Sleep(1000);
                            counter++;

                            return InstallInternal(serviceCommand, reinstalling, counter);
                        }

                        Console.WriteLine($"Cannot register service {ServiceFullName}, because it is still marked for deletion. " +
                                          "Please close all applications which might be using it (e.g. the Services console) and try again.");
                        return false;

                    case ErrorAccessIsDenied:
                        Console.WriteLine($"Cannot register service {ServiceFullName} due to insufficient privileges. Please use Administrator account to install the service.");
                        return false;

                    default:
                        Console.WriteLine($"Cannot register service {ServiceFullName}: { FormatServiceControlToolError(result) }");
                        return false;
                }
            }

            // a failure here leaves the service registered, just without the given setting
            private void Configure(string serviceName)
            {
                var descriptionResult = RunServiceControlTool(["description", serviceName, WindowServiceDescription]);
                if (descriptionResult.ExitCode != 0)
                    Console.WriteLine($"Service {ServiceFullName} was registered, but its description could not be set: { FormatServiceControlToolError(descriptionResult) }");

                // restart the service when it crashes or stops with an error (e.g. it fails to start or to restart)
                var restartAction = $"restart/{(long)RecoveryRestartDelay.TotalMilliseconds}";
                var failureResult = RunServiceControlTool([
                    "failure", serviceName,
                    "reset=", ((long)RecoveryResetPeriod.TotalSeconds).ToString(),
                    "actions=", string.Join("/", restartAction, restartAction, restartAction)
                ]);

                if (failureResult.ExitCode == 0)
                    failureResult = RunServiceControlTool(["failureflag", serviceName, "1"]);

                if (failureResult.ExitCode != 0)
                    Console.WriteLine($"Service {ServiceFullName} was registered, but its recovery actions could not be set: { FormatServiceControlToolError(failureResult) }");

                try
                {
                    SetPreshutdownTimeout(serviceName, PreshutdownTimeout);
                }
                catch (Exception e) when (e is Win32Exception or InvalidOperationException)
                {
                    var win32Exception = e as Win32Exception ?? e.InnerException as Win32Exception;
                    var message = win32Exception != null ? FormatWin32ErrorMessage(win32Exception) : e.Message;
                    Console.WriteLine($"Service {ServiceFullName} was registered, but its preshutdown timeout could not be set: {message}");
                }
            }

            // sc.exe has no command for the preshutdown timeout
            private static void SetPreshutdownTimeout(string serviceName, TimeSpan timeout)
            {
                using (var serviceController = new ServiceController(serviceName))
                using (var serviceHandle = serviceController.ServiceHandle)
                {
                    var info = new ServicePreshutdownInfo
                    {
                        PreshutdownTimeout = (uint)timeout.TotalMilliseconds
                    };

                    if (ChangeServiceConfig2(serviceHandle, ServiceConfigPreshutdownInfo, ref info) == false)
                        throw new Win32Exception(Marshal.GetLastPInvokeError());
                }
            }

            private const uint ServiceConfigPreshutdownInfo = 7;

            [StructLayout(LayoutKind.Sequential)]
            private struct ServicePreshutdownInfo
            {
                public uint PreshutdownTimeout;
            }

            [LibraryImport("advapi32.dll", EntryPoint = "ChangeServiceConfig2W", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            private static partial bool ChangeServiceConfig2(SafeHandle service, uint infoLevel, ref ServicePreshutdownInfo info);

            // ServiceController does not expose the binary path and the registry API does not support the longest (256 characters) service names
            private static string GetServiceBinaryPath(string serviceName)
            {
                var serviceManager = OpenSCManager(null, null, ScManagerConnect);
                if (serviceManager == IntPtr.Zero)
                    throw new Win32Exception(Marshal.GetLastPInvokeError());

                try
                {
                    var service = OpenService(serviceManager, serviceName, ServiceQueryConfig);
                    if (service == IntPtr.Zero)
                        throw new Win32Exception(Marshal.GetLastPInvokeError());

                    try
                    {
                        if (QueryServiceConfig(service, IntPtr.Zero, 0, out var bytesNeeded) == false && Marshal.GetLastPInvokeError() != ErrorInsufficientBuffer)
                            throw new Win32Exception(Marshal.GetLastPInvokeError());

                        var buffer = Marshal.AllocHGlobal(bytesNeeded);
                        try
                        {
                            if (QueryServiceConfig(service, buffer, bytesNeeded, out _) == false)
                                throw new Win32Exception(Marshal.GetLastPInvokeError());

                            var config = Marshal.PtrToStructure<QueryServiceConfigHeader>(buffer);
                            return Marshal.PtrToStringUni(config.BinaryPathName);
                        }
                        finally
                        {
                            Marshal.FreeHGlobal(buffer);
                        }
                    }
                    finally
                    {
                        CloseServiceHandle(service);
                    }
                }
                finally
                {
                    CloseServiceHandle(serviceManager);
                }
            }

            private const uint ScManagerConnect = 0x0001;

            private const uint ServiceQueryConfig = 0x0001;

            private const int ErrorInsufficientBuffer = 122;

            // the beginning of QUERY_SERVICE_CONFIGW
            [StructLayout(LayoutKind.Sequential)]
            private struct QueryServiceConfigHeader
            {
                public uint ServiceType;
                public uint StartType;
                public uint ErrorControl;
                public IntPtr BinaryPathName;
            }

            [LibraryImport("advapi32.dll", EntryPoint = "OpenSCManagerW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
            private static partial IntPtr OpenSCManager(string machineName, string databaseName, uint desiredAccess);

            [LibraryImport("advapi32.dll", EntryPoint = "OpenServiceW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
            private static partial IntPtr OpenService(IntPtr serviceManager, string serviceName, uint desiredAccess);

            [LibraryImport("advapi32.dll", EntryPoint = "QueryServiceConfigW", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            private static partial bool QueryServiceConfig(IntPtr service, IntPtr config, int bufferSize, out int bytesNeeded);

            [LibraryImport("advapi32.dll", SetLastError = true)]
            [return: MarshalAs(UnmanagedType.Bool)]
            private static partial bool CloseServiceHandle(IntPtr handle);

            private static string FormatWin32ErrorMessage(Win32Exception exception)
            {
                return $"{exception.Message} (ERROR CODE 0x{exception.NativeErrorCode:x8}).";
            }

            private static string NormalizeServiceName(string serviceName)
            {
                return Regex.Replace(serviceName, @"[\/\s]", "_");
            }

            private static string FormatServiceControlToolError((int ExitCode, string Output) result)
            {
                var message = string.IsNullOrWhiteSpace(result.Output) ? "sc.exe reported a failure." : result.Output;
                return $"{message} (ERROR CODE 0x{result.ExitCode:x8}).";
            }

            // The .NET BCL exposes no managed API to create/delete a Windows service, so register
            // and unregister shell out to sc.exe (the tool ships in %SystemRoot%\System32). sc.exe
            // returns the underlying Win32 error code as its process exit code, which lets the
            // callers branch on the same ERROR_* codes used elsewhere in this file.
            private static (int ExitCode, string Output) RunServiceControlTool(IReadOnlyList<string> arguments)
            {
                var startInfo = new ProcessStartInfo
                {
                    FileName = ServiceControlToolPath,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                foreach (var argument in arguments)
                    startInfo.ArgumentList.Add(argument);

                using (var process = Process.Start(startInfo))
                {
                    // read stderr concurrently, so neither of the redirected pipes can fill up and block sc.exe
                    var errorTask = process.StandardError.ReadToEndAsync();
                    var output = process.StandardOutput.ReadToEnd();
                    var error = errorTask.GetAwaiter().GetResult();
                    process.WaitForExit();

                    var combined = string.Join(
                        Environment.NewLine,
                        new[] { output, error }.Where(part => string.IsNullOrWhiteSpace(part) == false));

                    return (process.ExitCode, combined.Trim());
                }
            }

            public bool Uninstall()
            {
                using (var serviceController = GetServiceController())
                {
                    if (serviceController == null)
                    {
                        Console.WriteLine($"Service {ServiceFullName} does not exist. No action taken.");
                        return true;
                    }

                    if (IsRavenServerService(serviceController) == false)
                        return false;

                    return UninstallInternal(serviceController);
                }
            }

            // protects other services from being stopped and deleted when their name is passed by mistake
            private bool IsRavenServerService(ServiceController serviceController)
            {
                string binaryPath;
                try
                {
                    binaryPath = GetServiceBinaryPath(serviceController.ServiceName);
                }
                catch (Win32Exception e)
                {
                    Console.WriteLine($"Cannot check whether service {ServiceFullName} runs RavenDB Server: { FormatWin32ErrorMessage(e) } No action taken.");
                    return false;
                }

                if (binaryPath != null && binaryPath.Contains("Raven.Server", StringComparison.OrdinalIgnoreCase))
                    return true;

                Console.WriteLine($"Service {ServiceFullName} exists, but it does not run RavenDB Server ({binaryPath}). No action taken, please use a different service name.");
                return false;
            }

            private bool UninstallInternal(ServiceController serviceController)
            {
                if (StopInternal(serviceController) == false)
                    return false;

                var result = RunServiceControlTool(["delete", serviceController.ServiceName]);

                switch ((uint)result.ExitCode)
                {
                    case 0:
                        Console.WriteLine($"Service {ServiceFullName} has been unregistered.");
                        return true;

                    case ErrorAccessIsDenied:
                        Console.WriteLine($"Cannot unregister service {ServiceFullName} due to insufficient privileges. Please use Administrator account to uninstall the service.");
                        return false;

                    default:
                        Console.WriteLine($"Cannot unregister service {ServiceFullName}: { FormatServiceControlToolError(result) }");
                        return false;
                }
            }

            private bool StopInternal(ServiceController serviceController)
            {
                try
                {
                    var status = serviceController.Status;
                    if (status != ServiceControllerStatus.Stopped && status != ServiceControllerStatus.StopPending)
                    {
                        Console.WriteLine($"Service {ServiceFullName} is being stopped.");
                        serviceController.Stop();
                    }

                    if (WaitForStatus(serviceController, ServiceControllerStatus.Stopped, failedStatus: null) == false)
                    {
                        Console.WriteLine($"Service {ServiceFullName} did not stop within {StatusChangeTimeout.TotalSeconds} seconds.");
                        return false;
                    }

                    Console.WriteLine($"Service {ServiceFullName} stopped.");
                    return true;
                }
                catch (InvalidOperationException e) when (e.InnerException is Win32Exception win32Exception)
                {
                    Console.WriteLine($"Error stopping service {ServiceFullName}: { FormatWin32ErrorMessage(win32Exception) }");
                    return false;
                }
            }

            private bool StartInternal(ServiceController serviceController)
            {
                try
                {
                    var status = serviceController.Status;
                    if (status != ServiceControllerStatus.Running && status != ServiceControllerStatus.StartPending)
                    {
                        Console.WriteLine($"Service {ServiceFullName} is starting.");
                        serviceController.Start();
                    }

                    if (WaitForStatus(serviceController, ServiceControllerStatus.Running, failedStatus: ServiceControllerStatus.Stopped) == false)
                    {
                        Console.WriteLine(serviceController.Status == ServiceControllerStatus.Stopped
                            ? $"Service {ServiceFullName} failed to start. Please check the RavenDB logs and the System event log for details."
                            : $"Service {ServiceFullName} did not start within {StatusChangeTimeout.TotalSeconds} seconds.");
                        return false;
                    }

                    Console.WriteLine($"Service {ServiceFullName} started.");
                    return true;
                }
                catch (InvalidOperationException e) when (e.InnerException is Win32Exception win32Exception)
                {
                    Console.WriteLine($"Error starting service {ServiceFullName}: { FormatWin32ErrorMessage(win32Exception) }");
                    return false;
                }
            }

            private static bool WaitForStatus(ServiceController serviceController, ServiceControllerStatus desiredStatus, ServiceControllerStatus? failedStatus)
            {
                var sw = Stopwatch.StartNew();
                while (true)
                {
                    serviceController.Refresh();

                    var status = serviceController.Status;
                    if (status == desiredStatus)
                        return true;

                    if (status == failedStatus || sw.Elapsed > StatusChangeTimeout)
                        return false;

                    Thread.Sleep(250);
                }
            }

            public bool Start()
            {
                using (var serviceController = GetServiceController())
                {
                    if (serviceController == null)
                    {
                        Console.WriteLine($"Service {ServiceFullName} does not exist.");
                        return false;
                    }

                    return StartInternal(serviceController);
                }
            }

            public bool Stop()
            {
                using (var serviceController = GetServiceController())
                {
                    if (serviceController == null)
                    {
                        Console.WriteLine($"Service {ServiceFullName} does not exist.");
                        return false;
                    }

                    return StopInternal(serviceController);
                }
            }

            private string GetServiceCommand(string serverDir, List<string> argsForService)
            {
                using (var process = Process.GetCurrentProcess())
                {
                    var processExeFileName = process.MainModule.FileName;

                    serverDir = string.IsNullOrEmpty(serverDir)
                        ? new FileInfo(processExeFileName).Directory.FullName
                        : serverDir;

                    var serverDirInfo = new DirectoryInfo(serverDir);
                    if (serverDirInfo.Exists == false)
                        throw new ArgumentException($"Directory does not exist: {serverDir}.");
                    else
                        serverDir = serverDirInfo.FullName;

                    var serverExecutable = Path.Combine(serverDirInfo.FullName, "Raven.Server.exe");
                    if (File.Exists(serverExecutable) == false)
                    {
                        throw new ArgumentException($"Could not find RavenDB Server executable under {serverDirInfo.FullName}.");
                    }

                    if (argsForService.Any(x => x.StartsWith("--service-name")) == false)
                    {
                        argsForService.Add("--service-name");
                        argsForService.Add(_serviceName);
                    }

                    // Build the SCM ImagePath (executable + arguments) with the shared escaper so
                    // the executable path and each argument are quoted correctly, e.g. when the
                    // installation directory or an argument value contains spaces.
                    return CommandLineArgumentEscaper.EscapeAndConcatenate(argsForService.Prepend(serverExecutable));
                }
            }

            private ServiceController GetServiceController()
            {
                // service names are case insensitive
                var serviceName = NormalizeServiceName(_serviceName);
                return ServiceController.GetServices().FirstOrDefault(x => string.Equals(x.ServiceName, serviceName, StringComparison.OrdinalIgnoreCase));
            }
        }
    }
}
