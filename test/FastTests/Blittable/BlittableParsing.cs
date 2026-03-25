using System.IO;
using System.Text;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableParsing(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void CanParseProperly()
        {
            var json = "{\"Type\":\"Acknowledge\",\"Etag\":194}";
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 3; i++)
                {
                    using (var blittableJsonReaderObject = context.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "n"))
                    {
                        string s;
                        blittableJsonReaderObject.TryGet("Type", out s);
                        Assert.Equal("Acknowledge", s);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void BlittableShouldBeReadCorrectly()
        {
            // Test for RavenDB-25110 - Blittable corruption with CompressedLazyStringValue
            // Character 'a' is repeated 129 times to trigger CompressedLazyStringValue usage
            const string json = "{\n \"Exception\" : \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n}";

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (var blittableJsonReaderObject = context.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "n"))
                {
                    var result = context.ReadObject(blittableJsonReaderObject, null).ToString();

                    const string expected = "{\"Exception\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}";

                    Assert.Equal(expected, result);
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void BlittableShouldHandleRealWorldAlertJson()
        {
            // RavenDB-25340: JSON from notification that exposed the issue originally.
            const string json =
                @"{""Id"":""AlertRaised/LicenseManager_LeaseLicenseError""," +
                @"""CreatedAt"":""2025-11-12T12:35:16.2307307Z""," +
                @"""Type"":""AlertRaised""," +
                @"""Title"":""Failed to lease license""," +
                @"""Message"":""Could not lease license""," +
                @"""Severity"":""Warning""," +
                @"""IsPersistent"":true," +
                @"""Database"":null," +
                @"""Key"":null," +
                @"""AlertType"":""LicenseManager_LeaseLicenseError""," +
                @"""Details"":{" +
                @"""$type"":""Raven.Server.NotificationCenter.Notifications.Details.ExceptionDetails, Raven.Server""," +
                @"""Exception"":""System.InvalidOperationException: see exception details\r\n" +
                @" ---> System.Threading.Tasks.TaskCanceledException: The request was canceled due to the configured HttpClient.Timeout of 0.001 seconds elapsing.\r\n" +
                @" ---> System.TimeoutException: A task was canceled.\r\n" +
                @" ---> System.Threading.Tasks.TaskCanceledException: A task was canceled.\r\n" +
                @"   at System.Threading.Tasks.TaskCompletionSourceWithCancellation`1.WaitWithCancellationAsync(CancellationToken cancellationToken)\r\n" +
                @"   at System.Net.Http.HttpConnectionPool.SendWithVersionDetectionAndRetryAsync(HttpRequestMessage request, Boolean async, Boolean doRequestAuth, CancellationToken cancellationToken)\r\n" +
                @"   at System.Net.Http.RedirectHandler.SendAsync(HttpRequestMessage request, Boolean async, CancellationToken cancellationToken)\r\n" +
                @"   at System.Net.Http.HttpClient.<SendAsync>g__Core|83_0(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationTokenSource cts, Boolean disposeCts, CancellationTokenSource pendingRequestsCts, CancellationToken originalCancellationToken)\r\n" +
                @"   --- End of inner exception stack trace ---\r\n" +
                @"   --- End of inner exception stack trace ---\r\n" +
                @"   at System.Net.Http.HttpClient.HandleFailure(Exception e, Boolean telemetryStarted, HttpResponseMessage response, CancellationTokenSource cts, CancellationToken cancellationToken, CancellationTokenSource pendingRequestsCts)\r\n" +
                @"   at System.Net.Http.HttpClient.<SendAsync>g__Core|83_0(HttpRequestMessage request, HttpCompletionOption completionOption, CancellationTokenSource cts, Boolean disposeCts, CancellationTokenSource pendingRequestsCts, CancellationToken originalCancellationToken)\r\n" +
                @"   at Polly.Retry.AsyncRetryEngine.ImplementationAsync[TResult](Func`3 action, Context context, ExceptionPredicates shouldRetryExceptionPredicates, ResultPredicates`1 shouldRetryResultPredicates, Func`5 onRetryAsync, CancellationToken cancellationToken, Int32 permittedRetryCount, IEnumerable`1 sleepDurationsEnumerable, Func`4 sleepDurationProvider, Boolean continueOnCapturedContext)\r\n" +
                @"   at Polly.AsyncPolicy`1.ExecuteInternalAsync(Func`3 action, Context context, Boolean continueOnCapturedContext, CancellationToken cancellationToken)\r\n" +
                @"   at Raven.Server.Commercial.LicenseManager.GetUpdatedLicenseResponseMessage(License currentLicense, TransactionContextPool contextPool, CancellationToken token) in C:\\Users\\abc\\ravendb\\src\\Raven.Server\\Commercial\\LicenseManager.cs:line 650\r\n" +
                @"   at Raven.Server.Commercial.LicenseManager.GetUpdatedLicenseForActivation(License currentLicense) in C:\\Users\\abc\\ravendb\\src\\Raven.Server\\Commercial\\LicenseManager.cs:line 763\r\n" +
                @"   at Raven.Server.Commercial.LicenseManager.LeaseLicense(String raftRequestId, Boolean throwOnError) in C:\\Users\\abc\\ravendb\\src\\Raven.Server\\Commercial\\LicenseManager.cs:line 896\r\n" +
                @"   --- End of inner exception stack trace ---""}}";

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (var blittableJsonReaderObject = context.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "alert"))
                {
                    var result = context.ReadObject(blittableJsonReaderObject, null);

                    Assert.True(result.TryGet("Id", out string id));
                    Assert.Equal("AlertRaised/LicenseManager_LeaseLicenseError", id);

                    Assert.True(result.TryGet("Type", out string type));
                    Assert.Equal("AlertRaised", type);

                    Assert.True(result.TryGet("AlertType", out string alertType));
                    Assert.Equal("LicenseManager_LeaseLicenseError", alertType);

                    Assert.True(result.TryGet("Details", out BlittableJsonReaderObject details));
                    Assert.True(details.TryGet("Exception", out string exception));
                    Assert.Contains("System.InvalidOperationException", exception);
                    Assert.Contains("TaskCanceledException", exception);

                    var resultString = result.ToString();
                    Assert.Contains("AlertRaised/LicenseManager_LeaseLicenseError", resultString);
                    Assert.Contains("Failed to lease license", resultString);
                }
            }
        }
    }
}
