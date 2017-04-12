using System;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Server.Documents.Studio;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Web.Studio
{
    public class StudioFeedbackHandler : RequestHandler
    {
        [RavenAction("/studio/feedback", "POST")]
        public async Task Feedback()
        {
            FeedbackForm feedbackForm;

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                var json = context.Read(RequestBodyStream(), "feedback form");
                feedbackForm = JsonDeserializationServer.FeedbackForm(json);
            }

            await ServerStore.FeedbackSender.SendFeedback(feedbackForm).ConfigureAwait(false);

            NoContentStatus();
        }

    }
}