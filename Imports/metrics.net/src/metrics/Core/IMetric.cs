using System.Text;

namespace metrics.Core
{
    public interface IMetric : ICopyable<IMetric>
    {
        void LogJson(StringBuilder sb);
    }
}

        