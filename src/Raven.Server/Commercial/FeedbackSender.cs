using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Studio;
using Raven.Server.Utils;

namespace Raven.Server.Commercial
{
    public sealed class FeedbackSender
    {
        public async Task SendFeedback(FeedbackForm feedback)
        {
            var response = await ApiHttpClient.PostAsync("api/v1/feedback",
                    new StringContent(JsonStringHelper.Serialize(feedback), Encoding.UTF8, "application/json"))
                .ConfigureAwait(false);

            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
            {
                throw new InvalidOperationException(responseString);
            }
        }

    }
}
