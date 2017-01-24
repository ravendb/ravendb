using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Communication
{
    public interface ITransportBus : IDisposable
    {
        /// <summary>
        /// This method is used to obtain the underlining communication stream
        /// </summary>
        /// <returns>Return a readable/writable stream</returns>
        Stream GetStream();

        /// <summary>
        /// This method is used in order to give the thread, that is going to handle this communication, a human readable name.
        /// </summary>
        /// <returns>The node id trying to communicate with us</returns>
        string GetNodeId();

        /// <summary>
        /// This is will be the interface of the ITransportBus when implemented and the stream will be private 
        /// </summary>
        /// <returns></returns>
        IMessageHandler GetMessageHandler();
    }
}
