using System;
using System.Threading;
using System.Threading.Tasks;
using JenkinsNET;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Raven.Embedded;
using Redbus;
using Redbus.Interfaces;
using Vibrant.InfluxDB.Client;

namespace Tests.ResourceSnapshotAggregator
{
    public class Worker : IHostedService, IDisposable
    {
        private readonly ServiceSettings _settings;
        private readonly IEventBus _messageBus;
        private readonly IWebHost _jenkinsEndpointHost;
        private readonly ILogger<Worker> _logger;
        private readonly Task _hostTask;
        private TestSnapshotDataProcessor _dataProcessor;
        private SubscriptionToken _notificationsSubscription;
        private SubscriptionToken _notificationsCsvUpload;
        private const string TestResourceSnapshotsDatabase = "TestResourceSnapshots";

        public Worker(ServiceSettings settings, IEventBus messageBus, IWebHost jenkinsEndpointHost, ILogger<Worker> logger)
        {
            //TODO: add configuration support for server startup, perhaps customize stuff?
            EmbeddedServer.Instance.StartServer(new ServerOptions
            {
                AcceptEula = true,
                ServerUrl = "http://localhost:8080",
                DataDirectory = "Data"
            });

            if(bool.Parse(Environment.GetEnvironmentVariable("SNAPSHOT_AGGREGATOR_OPEN_BROWSER") ?? "false"))
                EmbeddedServer.Instance.OpenStudioInBrowser();

            _settings = settings;
            _messageBus = messageBus;
            _jenkinsEndpointHost = jenkinsEndpointHost;
            _logger = logger;
            _hostTask = _jenkinsEndpointHost.RunAsync();
        }

        public void ReceiveBuildNotification(BuildNotification buildNotification)
        {
            try
            {
                _dataProcessor.ProcessBuildNotification(buildNotification);
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to process build notification (job name = {buildNotification.JobName}, build number = {buildNotification.BuildNumber})");
            }
        }

        private void ReceiveCsvUpload(CsvUploadNotification csvUploadData)
        {
            try
            {
                _dataProcessor.ProcessBuildNotification(new BuildNotification
                {
                    JobName = csvUploadData.JobName,
                    BuildNumber = csvUploadData.BuildNumber
                },csvUploadData.Uploaded);
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"Failed to process build notification (job name = {csvUploadData.JobName}, build number = {csvUploadData.BuildNumber})");
            }
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (_hostTask.Status != TaskStatus.WaitingForActivation)
                await _jenkinsEndpointHost.StartAsync(cancellationToken);

            InitializeDataProcessor();
            _notificationsSubscription = _messageBus.Subscribe<BuildNotification>(ReceiveBuildNotification);
            _notificationsCsvUpload = _messageBus.Subscribe<CsvUploadNotification>(ReceiveCsvUpload);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _jenkinsEndpointHost.StopAsync(cancellationToken);
            _dataProcessor.Dispose();
            _messageBus.Unsubscribe(_notificationsSubscription);
            _messageBus.Unsubscribe(_notificationsCsvUpload);
        }

        private void InitializeDataProcessor()
        {
            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Url))
                _settings.Jenkins.Url = Environment.GetEnvironmentVariable("SNAPSHOT_AGGREGATOR_JENKINS_URL") ?? string.Empty;

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Username))
                _settings.Jenkins.Username = Environment.GetEnvironmentVariable("SNAPSHOT_AGGREGATOR_JENKINS_USERNAME") ?? string.Empty;

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Password))
                _settings.Jenkins.Password = Environment.GetEnvironmentVariable("SNAPSHOT_AGGREGATOR_JENKINS_PASSWORD") ?? string.Empty;

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.ApiKey))
                _settings.Jenkins.ApiKey = Environment.GetEnvironmentVariable("SNAPSHOT_AGGREGATOR_JENKINS_APIKEY") ?? string.Empty;

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Url))
                throw new InvalidOperationException("Jenkins URL isn't configured. Please use either settings json or a 'SNAPSHOT_AGGREGATOR_JENKINS_URL' environmental variable");

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Username))
                throw new InvalidOperationException("Jenkins Username isn't configured. Please use either settings json or a 'SNAPSHOT_AGGREGATOR_JENKINS_USERNAME' environmental variable");

            if (string.IsNullOrWhiteSpace(_settings.Jenkins.Password))
                throw new InvalidOperationException("Jenkins Password isn't configured. Please use either settings json or a 'SNAPSHOT_AGGREGATOR_JENKINS_PASSWORD' environmental variable");
            
            if (string.IsNullOrWhiteSpace(_settings.Jenkins.ApiKey))
                throw new InvalidOperationException("Jenkins API token isn't configured. Please use either settings json or a 'SNAPSHOT_AGGREGATOR_JENKINS_APIKEY' environmental variable");

            //TODO: add validation for InfluxDB settings as well

            try
            {
                _dataProcessor = new TestSnapshotDataProcessor(
                    _settings,
                    EmbeddedServer.Instance.GetDocumentStore(TestResourceSnapshotsDatabase),
                    new InfluxClient(new Uri(_settings.InfluxDB.Url), _settings.InfluxDB.Username, _settings.InfluxDB.Password), 
                    new JenkinsClient
                    {
                        BaseUrl = _settings.Jenkins.Url, 
                        UserName = _settings.Jenkins.Username, 
                        ApiToken = _settings.Jenkins.ApiKey
                    },
                    _logger);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to initialize the data processor.");
            }
        }

        public void Dispose()
        {
            if(_hostTask.Status == TaskStatus.RanToCompletion ||
               _hostTask.Status == TaskStatus.Faulted ||
               _hostTask.Status == TaskStatus.Canceled)
                _hostTask.Dispose();

            _dataProcessor?.Dispose();
            _jenkinsEndpointHost?.Dispose();
        }
    }
}
