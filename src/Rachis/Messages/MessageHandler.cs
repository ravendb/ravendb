using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Sparrow.Binary;

namespace Rachis.Messages
{
    public unsafe class MessageHandler
    {
        private byte[] _buff;
        private readonly Stream _stream;

        public MessageHandler(Stream stream)
        {
            _buff = new byte[8*1024];
            _stream = stream;
        }

        public MessageHeader ReadHeader()
        {
            ReadIntoBuffer(sizeof(MessageHeader));

            return MessageHeader.FromBytes(_buff,0);
        }

        public T ReadMessageBody<T>(MessageHeader header)
        {
            if (_buff.Length < header.Length)
            {
                _buff = new byte[Bits.NextPowerOf2(header.Length)];
            }          
            _stream.Read(_buff, 0, header.Length);
            var messageJson = Encoding.UTF8.GetString(_buff, 0, header.Length);
            return JsonConvert.DeserializeObject<T>(messageJson);
        }

        public T ReadMessage<T>()
        {
            return ReadMessageBody<T>(ReadHeader());
        }

        private void ReadIntoBuffer(int sizeToRead)
        {
            int totalRead = 0;
            while (totalRead < sizeToRead)
            {
                totalRead += _stream.Read(_buff, totalRead, sizeToRead - totalRead);
            }
        }

        public void WriteMessage(MessageType type,object message)
        {
            var m = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
            MessageHeader.ToBytes(_buff, 0, m.Length, type);
            _stream.Write(_buff,0,sizeof(MessageHeader));
            _stream.Write(m,0,m.Length);
            _stream.Flush();
        }


    }
}
