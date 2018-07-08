using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class CounterIncludesToken : QueryToken
    {
        private readonly string _sourcePath;
        private readonly string _parameterName;
        private readonly bool _all;

        private CounterIncludesToken(string sourcePath, string parameterName, bool all = false)
        {
            _parameterName = parameterName;
            _all = all;
            _sourcePath = sourcePath;
        }

        public static CounterIncludesToken Create(string sourcePath, string parameterName)
        {
            return new CounterIncludesToken(sourcePath, parameterName);
        }

        public static CounterIncludesToken All(string sourcePath)
        {
            return new CounterIncludesToken(sourcePath, null, true);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("counters(");

            if (_sourcePath != string.Empty)
            {
                writer
                    .Append(_sourcePath);

                if (_all == false)
                {
                    writer
                        .Append(", ");
                }
            }

            if (_all == false)
            {
                writer
                    .Append("$")
                    .Append(_parameterName);
            }

            writer
                .Append(")");
        }
    }
}
