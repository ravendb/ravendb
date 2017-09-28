using System;
using System.Linq;
using System.Management.Automation;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.OAuth;

namespace Raven.Powershell
{
    [Cmdlet(VerbsLifecycle.Request, "OAuthToken")]
    [OutputType(typeof(string))]
    public class OAuthAuthenticatorCmdlet : Cmdlet
    {
        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "Url of RavenDB server, including the port. Example --> http://localhost:8080")]
        public string ServerUrl { get; set; }

        [Parameter(ValueFromPipelineByPropertyName = true, Mandatory = true, HelpMessage = "ApiKey to use when fetching OAuth token. It should be full API key. Example --> key1/sAdVA0KLqigQu67Dxj7a")]
        public string ApiKey { get; set; }

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        protected override void StopProcessing()
        {
            _cts.Cancel();
        }

        protected override void ProcessRecord()
        {
            WriteProgress(new ProgressRecord(0, "Fetching RavenDB OAuth Token", "Please wait...")
            {
                RecordType = ProgressRecordType.Processing
            });

            var fetchTokenTask = TryObtainAuthToken(ServerUrl, ApiKey);

            try
            {
                fetchTokenTask.Wait(_cts.Token);
                WriteProgress(new ProgressRecord(0, "Fetching RavenDB OAuth Token", "Done.")
                {
                    RecordType = ProgressRecordType.Completed
                });
            }
            catch (AggregateException)
            {
            }
            catch (Exception ex)
            {
                WriteError(new ErrorRecord(ex, ex.Message, ErrorCategory.NotSpecified, fetchTokenTask));
            }

            if (fetchTokenTask.IsFaulted)
            {
                foreach (var ex in fetchTokenTask.Exception?.InnerExceptions ?? Enumerable.Empty<Exception>())
                {
                    OutputException(fetchTokenTask, ex);
                }
            }
            else
            {
                WriteObject(fetchTokenTask.Result);
            }
        }

        private void OutputException(Task<string> fetchTokenTask, Exception ex)
        {
            WriteError(new ErrorRecord(ex, ex.ToString(), ErrorCategory.NotSpecified, fetchTokenTask));
        }

        private static async Task<string> TryObtainAuthToken(string serverUrl, string apiKey)
        {
            using (var securedAuthenticator = new SecuredAuthenticator(autoRefreshToken: false))
            {
                var result = await securedAuthenticator.DoOAuthRequestAsync(null, serverUrl + "/OAuth/API-Key", apiKey);
                using (var httpClient = new HttpClient())
                {
                    result(httpClient);
                    var authenticationHeaderValue = httpClient.DefaultRequestHeaders.Authorization;
                    return authenticationHeaderValue.Parameter;
                }
            }
        }
    }
}
