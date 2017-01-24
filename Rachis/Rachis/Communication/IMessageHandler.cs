using Rachis.Messages;

namespace Rachis.Communication
{
    public interface IMessageHandler
    {
        MessageBase ReadMessage();
        void WriteMessage(MessageBase message);
    }
}