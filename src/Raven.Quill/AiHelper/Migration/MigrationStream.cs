using Microsoft.AspNetCore.Http;
using Raven.Quill.Endpoints.Helpers;

namespace Raven.Quill.AiHelper.Migration;

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
