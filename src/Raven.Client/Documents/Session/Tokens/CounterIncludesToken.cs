using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class CounterIncludesToken : QueryToken
    {
        private string _sourcePath;
        private readonly string _counterName;
        private readonly bool _all;

        private CounterIncludesToken(string sourcePath, string counterName, bool all = false)
        {
            _counterName = counterName;
            _all = all;
            _sourcePath = sourcePath;
        }

        public static CounterIncludesToken Create(string sourcePath, string counterName)
        {
            return new CounterIncludesToken(sourcePath, counterName);
        }

        public static CounterIncludesToken All(string sourcePath)
        {
            return new CounterIncludesToken(sourcePath, null, true);
        }

        public void AddAliasToPath(string alias)
        {
            _sourcePath = _sourcePath == string.Empty
                ? alias
                : $"{alias}.{_sourcePath}";
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("counters(");

            if (_sourcePath != string.Empty)
            {
                writer.Append(_sourcePath);

                if (_all == false)
                    writer.Append(", ");
            }

            if (_all == false)
            {
                writer.Append("'");
                writer.Append(_counterName);
                writer.Append("'");
            }

            writer.Append(")");
        }
    }
}
