using System;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public sealed class PerCoreStatic<T> where T : class
    {
        private readonly T[] _perCoreArrays;

        public PerCoreStatic(Func<T> creationFunc)
        {
            _perCoreArrays = new T[Environment.ProcessorCount];
            for (int i = 0; i < _perCoreArrays.Length; i++)
                _perCoreArrays[i] = creationFunc();
        }

        public T Get()
        {
            return _perCoreArrays[CurrentProcessorIdHelper.GetCurrentProcessorId() % _perCoreArrays.Length];
        }
    }
}
