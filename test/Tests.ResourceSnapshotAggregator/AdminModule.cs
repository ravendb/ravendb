using System.Collections.Generic;
using System.IO;
using Nancy;
using Redbus.Interfaces;

namespace Tests.ResourceSnapshotAggregator
{
    // ReSharper disable once UnusedMember.Global
    public class AdminModule : NancyModule
    {
        public AdminModule(IEventBus messageBus)
        {
            Post("/artifact-csv/{jobName}/{buildNum}", @params =>
            {
                if (string.IsNullOrWhiteSpace(@params.buildNum))
                    return Negotiate.WithModel("Missing build number from query parameters. Please include 'buildNum' in the query.")
                                    .WithStatusCode(HttpStatusCode.BadRequest);
                if (string.IsNullOrWhiteSpace(@params.jobName))
                    return Negotiate.WithModel("Missing job name from query parameters. Please include 'jobName' in the query.")
                                    .WithStatusCode(HttpStatusCode.BadRequest);

                var streamList = new List<Stream>();
                foreach (var file in Request.Files)
                {
                    var stream = new MemoryStream();
                    file.Value.CopyTo(stream);
                    streamList.Add(stream);
                }

                messageBus.Publish(new CsvUploadNotification(streamList)
                {
                    BuildNumber = @params.buildNum,
                    JobName = @params.JobName
                });
                
                return HttpStatusCode.OK;
            });
        }
    }
}
