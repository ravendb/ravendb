using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AutoMapper;
using CsvHelper;
using CsvHelper.Configuration;
using JenkinsNET;
using JenkinsNET.Models;
using Microsoft.CodeAnalysis.FlowAnalysis;
using Microsoft.Extensions.Logging;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using RestSharp;
using RestSharp.Authenticators;
using Tests.Infrastructure;
using Vibrant.InfluxDB.Client;

namespace Tests.ResourceSnapshotAggregator
{
    public class TestSnapshotDataProcessor : IDisposable
    {
        private readonly ServiceSettings _settings;
        private readonly IDocumentStore _store;
        private readonly InfluxClient _influxClient;
        private readonly JenkinsClient _jenkinsClient;
        private readonly ILogger<Worker> _logger;
        private readonly object _disposeLock = new object();

        public TestSnapshotDataProcessor(ServiceSettings settings, IDocumentStore store, InfluxClient influxClient, JenkinsClient jenkinsClient, ILogger<Worker> logger)
        {
            _settings = settings;
            _store = store;
            _influxClient = influxClient;
            _jenkinsClient = jenkinsClient;
            
            _logger = logger;
        }

        public void ProcessBuildNotification(BuildNotification notification, List<Stream> artifactStreams = null)
        {
            lock (_disposeLock)
            {
                var session = _store.OpenSession();
                
                session.Store(notification);
                
                try
                {
                    try
                    {
                        if (artifactStreams == null)
                        {
                            //TODO: use this implementation once HRINT-1455 is resolved (https://issues.hibernatingrhinos.com/issue/HRINT-1455)
                            //artifactStreams = ExtractAndStoreArtifacts(notification, session);

                            //until HRINT-1455 is resolved, this is a workaround code
                            artifactStreams = ExtractAndStoreArtifactsWorkaround(notification, session);
                        }
                        else
                        {
                            StoreRawStreams(notification, artifactStreams, session);
                        }
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e,"Failed to fetch artifacts from Jenkins. Cannot continue...");
                        return;
                    }

                    try
                    {
                        session.SaveChanges();
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Failed to write resource measurement snapshots to RavenDB (including artifacts).");
                    }

                    try
                    {
                        ProcessAndWriteToInfluxDb(notification.JobName, notification.BuildNumber, artifactStreams);
                    }
                    catch (Exception e)
                    {
                        _logger.LogError(e, "Failed to write resource measurement snapshots to InfluxDB.");
                    }
                }
                finally
                {
                    artifactStreams?.ForEach(d => d.Dispose());
                }
            }
        }

        //translate between rows of TestResourceSnapshotWriter.TestResourceSnapshot and ResourceUsageSnapshot - which is InfluxDB row
        private static readonly Mapper _influxRowMapper = new Mapper(
            new MapperConfiguration(cfg => 
                cfg.CreateMap<TestResourceSnapshotWriter.TestResourceSnapshot, ResourceUsageSnapshot>()));

        private void ProcessAndWriteToInfluxDb(string jobName, string buildNumber, List<Stream> artifactStreams)
        {
            foreach (var stream in artifactStreams)
            {
                stream.Position = 0;
                using var reader = new StreamReader(stream);
                using var csvReader = new CsvReader(reader,new Configuration
                {
                    BadDataFound = ctx => _logger.LogWarning($"Found bad data on field = '{ctx.Field}', CharPosition = {ctx.CharPosition}"),
                    IgnoreBlankLines = true,
                    HeaderValidated = (isValid, headerNames, index, ctx) => _logger.LogWarning($"Found missing headers ({string.Join(",", headerNames)}). This might not be an issue, if the fields of TestResourceSnapshot were changed."),
                    MissingFieldFound = (headerNames, index, ctx) => _logger.LogWarning($"Found missing fields ({string.Join(",", headerNames)})")
                });

                var records = csvReader.GetRecords<TestResourceSnapshotWriter.TestResourceSnapshot>().ToList();
                var influxRecords = 
                    records.Select(x =>
                    {
                        var row = _influxRowMapper.Map<ResourceUsageSnapshot>(x);
                        row.JobName = jobName; //not strictly needed, since this is the measurement name as well
                        row.BuildNumber = buildNumber;
                        return row;
                    }).ToList();

                try
                {
                    var deltas = SnapshotDelta.Calculate(influxRecords);
                    _influxClient.WriteAsync(_settings.InfluxDB.Database, jobName, influxRecords.Union(deltas), new InfluxWriteOptions {Precision = TimestampPrecision.Millisecond}).Wait();
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Failed to write test snapshot data to InfluxDB.");
                }
            }
        }

        private void StoreRawStreams(object entity ,List<Stream> streams, IDocumentSession session)
        {
            var inx = 1;
            foreach (var stream in streams)
            {
                stream.Position = 0;
                session.Advanced.Attachments.Store(entity, $"raw-csv/{inx++}", stream, "text/csv");
            }
        }

        private List<Stream> ExtractAndStoreArtifactsWorkaround(BuildNotification notification, IDocumentSession session)
        {
            var build = _jenkinsClient.Builds.Get<JenkinsBuildBase>(notification.JobName, notification.BuildNumber);
            var client = new RestClient(build.Url) {Authenticator = new HttpBasicAuthenticator(_settings.Jenkins.Username, _settings.Jenkins.Password)};
            
            var taskSource = new TaskCompletionSource<IRestResponse>();
            client.GetAsync(new RestRequest("/artifact/*zip*/archive.zip?token=" + _settings.Jenkins.ApiKey), (response, asyncHandle) =>
            {
               taskSource.SetResult(response);
            });
            taskSource.Task.Wait();

            using var artifactMemoryStream = new MemoryStream(taskSource.Task.Result.RawBytes);
            using var artifactsArchive = new ZipArchive(artifactMemoryStream);

            var artifactStreams = new List<Stream>();
            foreach (var entry in artifactsArchive.Entries)
            {
                if (!entry.Name.EndsWith(".csv", true, CultureInfo.InvariantCulture))
                    continue;

                var tempStream = new MemoryStream();
                artifactStreams.Add(tempStream);
                entry.Open().CopyTo(tempStream);
                tempStream.Position = 0;
                session.Advanced.Attachments.Store(notification, entry.Name, tempStream, "text/csv");
            }
            
            return artifactStreams;
        }

        private List<Stream> ExtractAndStoreArtifacts(BuildNotification notification, IDocumentSession session)
        {
            var build = _jenkinsClient.Builds.Get<JenkinsBuildBase>(notification.JobName, notification.BuildNumber);
            int fetchedArtifacts = 0;
            var artifactStreams = new List<Stream>();

            foreach (var artifact in build.Artifacts)
            {
                if (!artifact.FileName.EndsWith(".csv", StringComparison.InvariantCultureIgnoreCase))
                    continue;
                MemoryStream tempStream = null;
                try
                {
                    using var artifactStream = _jenkinsClient.Artifacts.Get(notification.JobName, notification.BuildNumber, artifact.FileName);
                    tempStream = new MemoryStream();

                    artifactStream.CopyTo(tempStream);
                    tempStream.Position = 0;
                    session.Advanced.Attachments.Store(notification, artifact.FileName, tempStream, "text/csv");
                }
                catch (Exception e) when (e.InnerException != null && e.InnerException is WebException webException)
                {
                    _logger.LogWarning(e.InnerException, $"Failed to fetch an artifact from Jenkins. (JobName = {notification.JobName}, BuildNumber = {notification.BuildNumber}, FileName = {artifact.FileName})");
                    tempStream?.Dispose();
                    continue;
                }

                fetchedArtifacts++;
            }

            if (fetchedArtifacts == 0)
                throw new InvalidOperationException(
                    $"Failed to fetch any artifacts from a build, so cannot continue. ((JobName = {notification.JobName}, BuildNumber = {notification.BuildNumber})");

            return artifactStreams;
        }

        public void Dispose()
        {
            lock (_disposeLock)
            {
                _store.Dispose();
                _influxClient.Dispose();
            }
        }
    }
}
