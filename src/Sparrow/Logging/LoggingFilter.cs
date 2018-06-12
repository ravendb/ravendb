using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Extensions;

namespace Sparrow.Logging
{
    public class LoggingFilter
    {
        private readonly Dictionary<LogEntryFields, List<Filter>> _rules =
            new Dictionary<LogEntryFields, List<Filter>>();

        public string Add(string rule, bool matchIsValid)
        {
            var parts = rule.Split(':');
            if (parts.Length != 2)
                return "Invalid rule, missing <header>:value";

            var key = parts[0];
            var value = parts[1];

            LogEntryFields result;
            if (Enum.TryParse(key, true, out result) == false)
                return "Unknown header field " + key;
            List<Filter> list;
            if (_rules.TryGetValue(result, out list) == false)
                _rules[result] = list = new List<Filter>();

            list.Add(new Filter
            {
                Value = value,
                MatchIsValid = matchIsValid
            });
            return "New rule added";
        }

        public string Delete(string rule)
        {
            var parts = rule.Split(':');
            var key = parts[0];
            int index;
            if (int.TryParse(parts[1], out index) == false)
                return "Invalid rule index " + parts[1];
            index -= 1;

            LogEntryFields result;
            if (Enum.TryParse(key, true, out result) == false)
                return "Unknown header field " + key;

            List<Filter> list;
            if (_rules.TryGetValue(result, out list) == false)
                return "No rules defined for " + result;

            if ((index >= 0) && (index < list.Count))
            {
                var ruleText = list[index];
                list.RemoveAt(index);
                return "Rule " + ruleText + " removed";
            }
            return "No rule at index " + index;
        }

        public string List()
        {
            var sb = new StringBuilder();
            foreach (var rule in _rules)
            {
                sb.Append(rule.Key).AppendLine();
                for (var index = 0; index < rule.Value.Count; index++)
                {
                    var r = rule.Value[index];
                    sb.Append("\t ")
                        .Append(index + 1)
                        .Append(". ")
                        .Append(r)
                        .AppendLine();
                }
            }
            return sb.ToString();
        }

        public string GetHelp()
        {
            var sb = new StringBuilder();
            sb.AppendLine("usage: filter [only] [except] [del] [-list]")
                .AppendLine();
            sb.AppendLine("The available headers are:");

            foreach (var entry in Enum.GetValues(typeof(LogEntryFields)))
                sb
                    .Append(entry + "  ");

            sb.AppendLine().AppendLine()
                .AppendLine("only <header>:somestring");
            sb.AppendLine().AppendLine()
                .AppendLine("except <header>:somestring");
            sb.AppendLine()
                .AppendLine("del <header>:[rule #]");
            return sb.ToString();
        }

        public bool IsValid(string val, List<string> rules)
        {
            foreach (var rule in rules)
                if (val.StartsWith(rule, StringComparison.Ordinal))
                    return true;
            return false;
        }

        private string GetField(LogEntryFields field, ref LogEntry entry)
        {
            switch (field)
            {
                case LogEntryFields.Time:
                    return entry.At.GetDefaultRavenFormat(isUtc: LoggingSource.UseUtcTime);
                case LogEntryFields.Source:
                    return entry.Source;
                case LogEntryFields.Logger:
                    return entry.Logger;
                case LogEntryFields.Message:
                    return entry.Message;
                case LogEntryFields.Exception:
                    return entry.Exception?.ToString();
                default:
                    return null;
            }
        }

        public bool Forward(ref LogEntry entry)
        {
            foreach (var rule in _rules)
            {
                foreach (var filter in rule.Value)
                {
                    var value = GetField(rule.Key, ref entry);
                    if (value == null)
                        return !filter.MatchIsValid;

                    var isMatch = value.IndexOf(filter.Value, StringComparison.OrdinalIgnoreCase) >= 0;
                    if (filter.MatchIsValid != isMatch)
                        return false;
                }
            }
            return true;
        }

        public string ParseInput(string input)
        {
            var args = input.Split(' ');
            var cmd = args[0];

            if (cmd != "filter")
                return "Unknown command " + cmd;
            if (args.Length < 2)
                return "No argument specified for filter";

            switch (args[1])
            {
                case "help":
                    return GetHelp();
                case "list":
                    return List();
                case "only":
                    if (args.Length != 3)
                        return "Invalid number of arguments for filter only";
                    return Add(args[2], true);
                case "except":
                    if (args.Length != 3)
                        return "Invalid number of arguments for filter except";
                    return Add(args[2], false);
                case "del":
                    if (args.Length != 3)
                        return "Invalid number of arguments for filter del ";
                    return Delete(args[2]);
            }
            return "Got it";
        }

        private enum LogEntryFields
        {
            Time,
            Source,
            Logger,
            Message,
            Exception
        }

        public class Filter
        {
            public bool MatchIsValid;
            public string Value;

            public override string ToString()
            {
                return (MatchIsValid ? "only" : "except") + " " + Value;
            }
        }
    }
}
