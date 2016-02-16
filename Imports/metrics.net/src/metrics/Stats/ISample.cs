using System.Collections.Generic;
using metrics.Core;

namespace metrics.Stats
{
    /// <summary>
    ///  A statistically representative sample of a data stream
    /// </summary>
    public interface ISample<out T> : ISample, ICopyable<T>
    {
        
    }

    /// <summary>
    ///  A statistically representative sample of a data stream
    /// </summary>
    public interface ISample
    {
        /// <summary>
        /// Clears all recorded values
        /// </summary>
        void Clear();

        /// <summary>
        /// Returns the number of values recorded
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Adds a new recorded value to the sample
        /// </summary>
        void Update(long value);

        /// <summary>
        ///  Returns a copy of the sample's values
        /// </summary>
        ICollection<long> Values { get; }
    }
}


