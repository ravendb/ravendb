namespace Raven.Quill.Hosting;

public static class ExpiryGateMiddleware
{
    private const string HealthPath = "/healthz";

    public static IApplicationBuilder UseExpiryGate(this WebApplication app)
    {
        IApplicationBuilder pipeline = app;

        var expiry = app.Services.GetRequiredService<IQuillExpiry>();
        if (expiry.IsExpired == false)
            return pipeline;

        // every route, static asset, embed link and websocket
        // upgrade answers the notice, and there is no `next` to forget to skip.
        pipeline.Run(async context =>
        {
            // 200 by choice, so the container keeps reporting healthy.
            if (string.Equals(context.Request.Path.Value, HealthPath, StringComparison.OrdinalIgnoreCase))
            {
                context.Response.ContentType = "text/plain";
                await context.Response.WriteAsync("expired");
                return;
            }

            var notice = context.RequestServices.GetRequiredService<ExpiryNotice>();

            context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            context.Response.ContentType = "text/html; charset=utf-8";
            await context.Response.WriteAsync(notice.Page);
        });

        return pipeline;
    }
}
