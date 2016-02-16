using System;
using System.Runtime.Serialization;
using System.Text;

namespace metrics.Core
{
    /// <summary>
    /// An untyped version of a gauge for reporting purposes
    /// </summary>
    public abstract class GaugeMetric
    {
        public abstract string ValueAsString { get; }
    }

    /// <summary>
    /// A gauge metric is an instantaneous reading of a particular value. To
    /// instrument a queue's depth, for example:
    /// <example>
    /// <code> 
    /// var queue = new Queue{int}();
    /// var gauge = new GaugeMetric{int}(() => queue.Count);
    /// </code>
    /// </example>
    /// </summary>
    public sealed class GaugeMetric<T> : GaugeMetric, IMetric
    {
        private readonly Func<T> _evaluator;

        public GaugeMetric(Func<T> evaluator)
        {
            _evaluator = evaluator;
        }

        public T Value
        {
            get { return _evaluator.Invoke(); }
        }

        public override string ValueAsString
        {
            get { return Value.ToString(); }
        }

        [IgnoreDataMember]
        public IMetric Copy
        {
            get { return new GaugeMetric<T>(_evaluator); }
        }

        public void LogJson(StringBuilder sb)
        {
            sb.Append("{\"value\":").Append(Value).Append("}");
     
        }
    }
}