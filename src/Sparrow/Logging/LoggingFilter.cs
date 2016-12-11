using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;

namespace Sparrow.Logging
{
    public class LoggingFilter
    {

            private readonly Dictionary<string, List<string>> _rules = new Dictionary<string, List<string>>();
            private readonly string[] _entryFields = {"time","source","type","message","exception"};

            public LoggingFilter()
            {
                foreach (var entry in _entryFields)
                {
                    _rules.Add(entry, new List<string>());
                }
            }

            public void Add(string rule)
            {
                string[] parts = rule.Split(':');
                string key = parts[0];
                string value = parts[1];

                if (_entryFields.Contains(key) && value.Length > 0)
                {
                    _rules[key].Add(value.ToLower());
                }
            }
            public void Delete(string rule)
            {
                string[] parts = rule.Split(':');
                string key = parts[0];
                int index = int.Parse(parts[1]) - 1;

                if (_entryFields.Contains(key) && index < _rules[key].Count)
                {
                    _rules[key].RemoveAt(index);
                }
            }
            public string List()
            {
                StringBuilder sb = new StringBuilder();
                foreach (var rule in _rules)
                {
                    sb.AppendLine("-" + rule.Key);
                    for (int index = 0; index < rule.Value.Count; index++)
                    {
                        var r = rule.Value[index];
                        sb.AppendLine("\t " + (index + 1) + ". " + r);
                    }
                }
                return sb.ToString();
            }

            public string GetHelp()
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("usage: filter [-add] [-del] [-list]\n");
                sb.AppendLine("The available headers are:");

                foreach (var entry in _entryFields)
                {
                    sb.Append(entry + "  ");
                }
                
                sb.AppendLine("\n\n-add <header>:somestring");
                sb.AppendLine("-del <header>:rule number");
            return sb.ToString();
            }
            public bool Forward(LogEntry entry)
            {
                bool valid = true;
              
                foreach (var e in _entryFields)
                {
                    var listOfRules = _rules[e];
                    if (listOfRules.Count != 0)
                    {
                    valid = false;
                    switch (e)
                        {
                            case "time":
                                var time = entry.At.GetDefaultRavenFormat(true).ToLower();
                                valid = listOfRules.Any(rule => time.StartsWith(rule));
                                break;
                            case "source":
                                var source = entry.Source.ToLower();
                                valid = listOfRules.Any(rule => source.StartsWith(rule));
                                break;
                            case "logger":
                                var logger = entry.Logger.ToLower();
                                valid = listOfRules.Any(rule => logger.StartsWith(rule));
                                break;
                            case "message":
                                var message = entry.Message.ToLower();
                                valid = listOfRules.Any(rule => message.StartsWith(rule));
                                break;
                            case "exception":
                                if (entry.Exception != null)
                                {
                                    var msg = entry.Exception.Message.ToLower();
                                    valid = listOfRules.Any(rule => msg.StartsWith(rule));
                                }                                
                                break;
                        }
                        if (valid)
                        {
                            return true;
                        }
                    }
                }
                return valid;
            }

            public string ParseInput(string input)
            {
                string[] args = input.Split(' ');
                string cmd = args[0];

                if (cmd == "filter")
                {
                    for (int i = 1; i < args.Length; i++)
                    {
                        switch (args[i])
                        {
                            case ("-help"):
                                return GetHelp();
                            case ("-list"):
                                return List();
                            case ("-add"):
                                Add(args[++i]);
                                break;
                            case ("-del"):
                                Delete(args[++i]);
                                break;
                        }
                    }
                }
                return "Got it";
            }

        }

   }
