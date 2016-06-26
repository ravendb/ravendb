using System.IO;

namespace Sparrow.Logging
{
    public class SingleProducerSingleConsumerCircularQueue
    {
        private readonly MemoryStream[] _data;
        private readonly int _queueSize;
        private volatile uint _readPos;
        private volatile uint _writePos;

        public SingleProducerSingleConsumerCircularQueue(int queueSize)
        {
            _queueSize = queueSize;
            _data = new MemoryStream[_queueSize];
        }

        private int PositionToArrayIndex(uint pos)
        {
            return (int)(pos%_queueSize);
        }

        public bool Enqueue(MemoryStream entry)
        {
            var readIndex = PositionToArrayIndex(_readPos);
            var writeIndex = PositionToArrayIndex(_writePos + 1);

            if (readIndex == writeIndex)
                return false; // queue full

            _data[PositionToArrayIndex(_writePos)] = entry;
            _writePos++;
            return true;
        }

        public bool Dequeue(out MemoryStream entry)
        {
            entry = null;
            var readIndex = PositionToArrayIndex(_readPos);
            var writeIndex = PositionToArrayIndex(_writePos);

            if (readIndex == writeIndex)
                return false; // queue empty

            entry = _data[readIndex];
            _data[readIndex] = null;

            _readPos++;

            return true;
        }
    }
}