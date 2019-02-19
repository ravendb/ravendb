using System;
using System.Text;
using Jint.Runtime;

namespace Raven.Server.Documents.Indexes.Static
{
    [Serializable]
    internal class JavaScriptIndexFuncException : Exception
    {
        public JavaScriptIndexFuncException()
        {
        }

        public JavaScriptIndexFuncException(string message) : base(message)
        {
        }

        public JavaScriptIndexFuncException(string message, Exception innerException) : base(message, innerException)
        {
        }

        private static readonly string[] _newLineSplitters = new[] { "\r\n", "\n", "\r" }; //TODO: check if Jint is using only \r\n

        static internal (string Message, bool Success) PrepareErrorMessageForJavaScriptIndexFuncException(string script, JavaScriptException jse)
        {
            var lines = script.Split(_newLineSplitters, StringSplitOptions.None);
            if (jse.Location.Start.Line > lines.Length || jse.Location.End.Line > lines.Length || jse.Location.Start.Line > jse.Location.End.Line)
                return (null, false);
            var sb = new StringBuilder();
            //Location seems to be '1' based
            var start = Math.Max(jse.Location.Start.Line - 1, 1);
            if (start > 1)
            {
                sb.AppendLine("...");
            }
            for (var i = start; i <= start + 2 && i <= lines.Length; i++)
            {
                var line = lines[i - 1];
                sb.AppendLine(line);

                if (i == jse.Location.Start.Line)
                {
                    sb.Append(new string('-', Math.Max(jse.Location.Start.Column - 1, 0)));
                    sb.Append(new string('^', 3));
                    sb.AppendLine(new string('-', Math.Max(5, line.Length - jse.Location.Start.Column + 1)));
                }
            }
            if (start + 2 < lines.Length)
            {
                sb.AppendLine("...");
            }
            var error = sb.ToString();
            return (error, true);
        }
    }
}
