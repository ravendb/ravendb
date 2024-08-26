using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Https
{
    public sealed class ExternalCertificateValidator
    {
        private readonly RavenServer _server;
        private readonly RavenLogger _logger;

        private ConcurrentDictionary<Key, Task<CachedValue>> _externalCertificateValidationCallbackCache;

        public ExternalCertificateValidator(RavenServer server, RavenLogger logger)
        {
            _server = server;
            _logger = logger;
        }

        public void Initialize()
        {
            if (string.IsNullOrEmpty(_server.Configuration.Security.CertificateValidationExec))
                return;
            
            _externalCertificateValidationCallbackCache = new ConcurrentDictionary<Key, Task<CachedValue>>();

            RequestExecutor.RemoteCertificateValidationCallback += (sender, cert, chain, errors) => ExternalCertificateValidationCallback(sender, cert, chain, errors, _logger);
        }

        public void ClearCache()
        {
            _externalCertificateValidationCallbackCache?.Clear();
        }
        private CachedValue CheckExternalCertificateValidation(string senderHostname, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors, RavenLogger log)
        {
            var base64Cert = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));

            var timeout = _server.Configuration.Security.CertificateValidationExecTimeout.AsTimeSpan;

            var args = $"{_server.Configuration.Security.CertificateValidationExecArguments ?? string.Empty} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(senderHostname)} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(base64Cert)} " +
                       $"{CommandLineArgumentEscaper.EscapeSingleArg(sslPolicyErrors.ToString())}";

            var startInfo = new ProcessStartInfo
            {
                FileName = _server.Configuration.Security.CertificateValidationExec,
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            var sw = Stopwatch.StartNew();
            Process process;
            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to execute '{_server.Configuration.Security.CertificateValidationExec} {args}'. Failed to start process.", e);
            }

            var readStdOut = process.StandardOutput.ReadToEndAsync();
            var readErrors = process.StandardError.ReadToEndAsync();

            string GetStdError()
            {
                try
                {
                    return readErrors.Result;
                }
                catch (Exception e)
                {
                    return $"Unable to get stderr, got exception: {e}";
                }
            }

            string GetStdOut()
            {
                try
                {
                    return readStdOut.Result;
                }
                catch (Exception e)
                {
                    return $"Unable to get stdout, got exception: {e}";
                }
            }

            if (process.WaitForExit((int)timeout.TotalMilliseconds) == false)
            {
                process.Kill();
                throw new InvalidOperationException($"Unable to execute '{_server.Configuration.Security.CertificateValidationExec} {args}', waited for {(int)timeout.TotalMilliseconds} ms but the process didn't exit. Output: {GetStdOut()}{Environment.NewLine}Errors: {GetStdError()}");
            }

            try
            {
                readStdOut.Wait(timeout);
                readErrors.Wait(timeout);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to read redirected stderr and stdout when executing '{_server.Configuration.Security.CertificateValidationExec} {args}'", e);
            }

            string output = GetStdOut();
            string errors = GetStdError();

            // Can have exit code 0 (success) but still get errors. We log the errors anyway.
            if (log.IsInfoEnabled)
                log.Info($"Executing '{_server.Configuration.Security.CertificateValidationExec} {args}' took {sw.ElapsedMilliseconds:#,#;;0} ms. Exit code: {process.ExitCode}{Environment.NewLine}Output: {output}{Environment.NewLine}Errors: {errors}{Environment.NewLine}");

            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Command or executable '{_server.Configuration.Security.CertificateValidationExec} {args}' failed. Exit code: {process.ExitCode}{Environment.NewLine}Output: {output}{Environment.NewLine}Errors: {errors}{Environment.NewLine}");
            }

            if (bool.TryParse(output, out bool result) == false)
            {
                throw new InvalidOperationException(
                    $"Cannot parse to boolean the result of Command or executable '{_server.Configuration.Security.CertificateValidationExec} {args}'. Exit code: {process.ExitCode}{Environment.NewLine}Output: {output}{Environment.NewLine}Errors: {errors}{Environment.NewLine}");
            }

            return new CachedValue { Valid = result, Until = result ? DateTime.UtcNow.AddMinutes(15) : DateTime.UtcNow.AddSeconds(30) };
        }

        public bool ExternalCertificateValidationCallback(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors, RavenLogger log)
        {
            var senderHostname = RequestExecutor.ConvertSenderObjectToHostname(sender);

            var cacheKey = new Key(senderHostname, certificate.GetCertHashString(), sslPolicyErrors);

            Task<CachedValue> task;
            if (_externalCertificateValidationCallbackCache.TryGetValue(cacheKey, out var existingTask) == false)
            {
                task = new Task<CachedValue>(() => CheckExternalCertificateValidation(senderHostname, certificate, chain, sslPolicyErrors, log));
                existingTask = _externalCertificateValidationCallbackCache.GetOrAdd(cacheKey, task);
                if (existingTask == task)
                {
                    task.Start();

                    if (_externalCertificateValidationCallbackCache.Count > 50)
                    {
                        foreach (var item in _externalCertificateValidationCallbackCache.Where(x => x.Value.IsCompleted).OrderBy(x => x.Value.Result.Until).Take(25))
                        {
                            _externalCertificateValidationCallbackCache.TryRemove(item.Key, out _);
                        }
                    }
                }
            }

            CachedValue cachedValue;
            try
            {
                cachedValue = existingTask.Result;
            }
            catch
            {
                _externalCertificateValidationCallbackCache.TryRemove(cacheKey, out _);
                throw;
            }

            if (_server.Time.GetUtcNow() < cachedValue.Until)
                return cachedValue.Valid;

            var cachedValueNext = cachedValue.Next;
            if (cachedValueNext != null)
                return ReturnTaskValue(cachedValueNext);

            task = new Task<CachedValue>(() =>
                CheckExternalCertificateValidation(senderHostname, certificate, chain, sslPolicyErrors, log));

            var nextTask = Interlocked.CompareExchange(ref cachedValue.Next, task, null);
            if (nextTask != null)
                return ReturnTaskValue(nextTask);


            task.ContinueWith(done =>
            {
                _externalCertificateValidationCallbackCache.TryUpdate(cacheKey, done, existingTask);
            }, TaskContinuationOptions.OnlyOnRanToCompletion);

            task.Start();

            return cachedValue.Valid; // we are computing this, but may take some time, let's use cached value for now

            bool ReturnTaskValue(Task<CachedValue> task)
            {
                if (task.IsCompletedSuccessfully)
                    return task.Result.Valid;

                // not done yet? return the cached value
                return cachedValue.Valid;
            }
        }

        private sealed class CachedValue
        {
            public DateTime Until;
            public bool Valid;

            public Task<CachedValue> Next;
        }

        private sealed class Key
        {
            public readonly string Host;
            public readonly string Cert;
            public readonly SslPolicyErrors Errors;

            public Key(string host, string cert, SslPolicyErrors errors)
            {
                Host = host;
                Cert = cert;
                Errors = errors;
            }

            private bool Equals(Key other)
            {
                return Host == other.Host && Cert == other.Cert && Errors == other.Errors;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != this.GetType())
                    return false;
                return Equals((Key)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = (Host != null ? Host.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (Cert != null ? Cert.GetHashCode() : 0);
                    hashCode = (hashCode * 397) ^ (int)Errors;
                    return hashCode;
                }
            }
        }
    }
}
