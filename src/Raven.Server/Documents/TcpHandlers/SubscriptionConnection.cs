namespace Raven.Server.Documents.BulkInsert
{
    public class SubscriptionConnection
    {
        /*
        [RavenAction("/databases/ * /        subscriptions/pull", "GET",
            "/databases/{databaseName:string}/subscriptions/pull?id={subscriptionId:long}&connection={connection:string}&strategy={strategy:string}&maxDocsPerBatch={maxDocsPerBatch:int}&maxBatchSize={maxBatchSize:int|optional}")]
        public async Task Pull()
        {
            var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);

            try
            {
                using (var subscriptionConnection = new SubscriptionWebSocketConnection(ContextPool, webSocket, Database))
                {
                    await subscriptionConnection.InitConnection(GetLongQueryString("id"), GetStringQueryString("connection"), GetStringQueryString("Strategy"), GetIntValueQueryString("maxDocsPerBatch"), GetIntValueQueryString("maxBatchSize", false));
                    await subscriptionConnection.Proccess();
                }
            }
            catch (Exception e)
            {
                // uncomment this when websockets mutual closure issue fixed in rc4
                //try
                //{
                //    if (webSocket.State == WebSocketState.Open)
                //    {
                //        await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
                //            // TODO: Replace this with real error generation
                //            "{'Type':'Error', 'Exception':'" + e.ToString().Replace("'", "\\'") + "'}",
                //            Database.DatabaseShutdown).ConfigureAwait(false);
                //    }
                //}
                //catch
                //{
                //    // ignored
                //}

                Log.ErrorException($"Failure in subscription id {GetLongQueryString("id")}", e);
            }
            finally
            {
                webSocket.Dispose();
            }
        }
*/
    }
}