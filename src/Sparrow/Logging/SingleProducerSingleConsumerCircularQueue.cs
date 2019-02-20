using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Sparrow.Logging
{
    public class WebSocketMessageEntry
    {
        public MemoryStream Data;
        public TaskCompletionSource<object> Task;

        public readonly List<WebSocket> WebSocketsList = new List<WebSocket>();

        public override string ToString()
        {
            return Encodings.Utf8.GetString(Data.ToArray());
        }
    }

    public class SingleProducerSingleConsumerCircularQueue<T>
    {
        private readonly SingleConsumerRingBuffer<T> _buffer;

        public SingleProducerSingleConsumerCircularQueue(int queueSize)
        {
            _buffer = new SingleConsumerRingBuffer<T>(queueSize);
        }

        public bool Enqueue(T entry)
        {
            return _buffer.TryPush(ref entry);
        }

        private int _numberOfTimeWaitedForEnqueue;

        public bool Enqueue(T entry, int timeout)
        {
            if (_buffer.TryPush(ref entry))
            {
                _numberOfTimeWaitedForEnqueue = 0;
                return true;
            }
            while (timeout > 0)
            {
                _numberOfTimeWaitedForEnqueue++;
                var timeToWait = _numberOfTimeWaitedForEnqueue / 2;
                if (timeToWait < 2)
                    timeToWait = 2;
                else if (timeToWait > timeout)
                    timeToWait = timeout;
                timeout -= timeToWait;
                Thread.Sleep(timeToWait);
                if (_buffer.TryPush(ref entry))
                    return true;
            }
            return false;
        }

        public bool Dequeue(out T entry)
        {
            if (_buffer.TryAcquireSingle(out RingItem<T> item))
            {
                entry = item.Item;

                _buffer.Release();
                return true;
            }
            
            entry = default(T);
            return false;
        }
    }
}
