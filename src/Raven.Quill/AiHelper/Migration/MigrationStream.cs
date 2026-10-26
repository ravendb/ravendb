using Microsoft.AspNetCore.Http;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// The browser's side of a planner turn. The response headers go out with the first frame, so a request
/// refused before the turn starts still answers with a status code. Writing stops once the browser is gone;
/// the turn itself runs on in the api, and the browser re-syncs with plan/get when it comes back.
/// </summary>
public sealed class MigrationStream(HttpContext context)
{
    public bool Started { get; private set; }

    public bool BrowserGone { get; private set; }

    public async Task WriteAsync(MigrationFrame frame)
    {
        if (BrowserGone)
            return;

        try
        {
            if (Started == false)
            {
                NdjsonStream.SetHeaders(context);
                Started = true;
            }

            await NdjsonStream.WriteLineAsync(context, frame);
        }
        catch (Exception e) when (e is OperationCanceledException or IOException)
        {
            BrowserGone = true;
        }
    }
}
