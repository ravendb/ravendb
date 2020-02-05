using System;
using System.Linq;
using Nancy;
using Nancy.Extensions;
using Raven.Embedded;
using Redbus.Interfaces;
using Utf8Json;

namespace Tests.ResourceSnapshotAggregator
{
    public class JenkinsEndpoint : NancyModule
    {
        public JenkinsEndpoint(IEventBus messageBus)
        {
            Post("/notify-build", @params =>
            {
                try
                {
                    var buildNotification = JsonSerializer.Deserialize<BuildNotification>(Request.Body.AsString());

                    var missingFields = buildNotification.CheckMissingFields().ToList();
                    if (missingFields.Any())
                        return Negotiate.WithModel($"One or more required fields are missing: {string.Join(",", missingFields)}")
                                        .WithStatusCode(HttpStatusCode.NotAcceptable);

                    messageBus.Publish(buildNotification);
                }
                catch (Exception e)
                {
                    return Negotiate.WithModel($"Failed to deserialize message. {Environment.NewLine} Raw message: {Request.Body.AsString()} {Environment.NewLine} Exception: {e}")
                                    .WithStatusCode(HttpStatusCode.BadRequest);
                }

                return Negotiate.WithStatusCode(HttpStatusCode.OK);
            });
        }
    }
}
