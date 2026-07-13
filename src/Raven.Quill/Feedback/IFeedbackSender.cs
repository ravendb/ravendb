using Raven.Quill.Contracts;

namespace Raven.Quill.Feedback;

public interface IFeedbackSender
{
    Task<bool> SendAsync(SendFeedbackRequest request, string userAgent, CancellationToken token);
}
