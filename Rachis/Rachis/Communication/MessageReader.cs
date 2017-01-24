using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Rachis.Messages;
using Sparrow.Json;

namespace Rachis.Communication
{
    /// <summary>
    /// This class is used for now to read messages from the ITransportBus, it will be merged into the implementation of the ITransportBus when it is ready.
    /// </summary>
    public unsafe class MessageHandler 
    {
        private byte[] buffer; //TODO: need to change this to use a context allocated buffer
        private static int sizeOfHeader = sizeof(MessageHeader);
        private int sizeOfBuffer = 1024;
        private Stream _stream;
        public MessageHandler(ITransportBus transport)
        {
            buffer = new byte[sizeOfBuffer];
            _stream = transport.GetStream();
        }

        //TODO: this should be read as blittable
        public MessageBase ReadMessage()
        {
            if (ReadIntoBuffer(sizeOfHeader) == false)
                return null;
            int length;
            MessageType type;
            fixed (byte* buf = buffer)
            {                
                length = ((MessageHeader*)buf)->Length;
                type = ((MessageHeader*)buf)->Type;
            }
            //we don't have enough buffer space to read from stream, need to increase the size of the buffer.
            if (length > sizeOfBuffer)
            {
                sizeOfBuffer = Sparrow.Binary.Bits.NextPowerOf2(length);
                //TODO:return buffer to context here
                buffer = new byte[sizeOfBuffer];
            }

            ReadIntoBuffer(length);
            
            switch (type)
            {
                case MessageType.AppendEntries:
                    return MessageBase.FromBytes<AppendEntries>(buffer,0,length);
                case MessageType.AppendEntriesResponse:
                    return MessageBase.FromBytes<AppendEntriesResponse>(buffer, 0, length);
                case MessageType.RequestVote:
                    return MessageBase.FromBytes<RequestVote>(buffer, 0, length);
                case MessageType.RequestVoteResponse:
                    return MessageBase.FromBytes<RequestVoteResponse>(buffer, 0, length);
                default:
                    throw new ArgumentOutOfRangeException();
            }

        }

        private bool ReadIntoBuffer(int sizeToRead)
        {
            var totalRead = 0;
            var position = 0;
            while (totalRead != sizeToRead)
            {
                var read = _stream.Read(buffer, position, sizeToRead - totalRead);
                if (read == 0)
                    return false;
                totalRead += read;
                position += read;
            }
            return true;
        }

        public void WriteMessage(MessageBase message)
        {
            //TODO: this should be written as blittable
            var bytes = message.ToBytes();
            var header = new MessageHeader {Length = bytes.Length, Type = message.GetMessageType()};
            var headerBytes = System.Text.Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(header));
            _stream.Write(headerBytes,0, headerBytes.Length);
            _stream.Write(bytes,0,bytes.Length);
        }
    }
}
