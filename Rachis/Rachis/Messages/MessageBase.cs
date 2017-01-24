using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Messages
{

    public abstract class MessageBase
    {
        /// <summary>
        /// This message is used to deserialize a message from a byte[]
        /// </summary>
        /// <param name="buffer">Holds the serialized object</param>
        /// <param name="start">Start position for the deserialization</param>
        /// <param name="length">The length of the object to deserialize</param>
        /// <returns>The message.</returns>
        public static T FromBytes<T>(byte[] buffer, int start, int length)
        {
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(System.Text.Encoding.UTF8.GetString(buffer, 0, length));
        }

        /// <summary>
        /// This method serialize a message into a byte[]
        /// </summary>
        /// <returns>The serialized message as byte[]</returns>
        public virtual byte[] ToBytes()
        {
            return System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.ToString(this));
        }

        public override string ToString()
        {
            return Newtonsoft.Json.JsonConvert.ToString(this);
        }

        public abstract MessageType GetMessageType();
    }

    public abstract class InitialMessageBase : MessageBase
    {
        public Guid ClusterTopologyId { get; set; }
        public string From { get; set; }
    }
}
