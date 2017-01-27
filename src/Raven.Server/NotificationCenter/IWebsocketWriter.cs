using System.Threading.Tasks;

namespace Raven.Server.NotificationCenter
{
    public interface IWebsocketWriter
    {
        Task WriteToWebSocket<TNotification>(TNotification notification);
    }
}