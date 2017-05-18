// -----------------------------------------------------------------------
//  <copyright file="TrafficRecorderConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net;
using NDesk.Options;
using Raven.Client.Util;

namespace Raven.Traffic
{
    public class TrafficToolConfiguration
    {
        public RavenConnectionStringOptions ConnectionString { get; private set; }
        public string ResourceName { get; set; }
        public TrafficToolMode Mode { get; set; }
        public string RecordFilePath { get; set; }
        public TimeSpan Timeout { get; set; }
        public string ApiKey { get; set; }
        public bool IsCompressed { get; set; }
        public bool PrintOutput { get; set; }
        public int? AmountConstraint { get; set; }
        public TimeSpan? DurationConstraint { get; set; }

        public TrafficToolConfiguration()
        {
            ConnectionString = new RavenConnectionStringOptions();
            IsCompressed = false;
            Timeout = TimeSpan.MinValue;
            PrintOutput = true;
        }

        public enum TrafficToolMode
        {
            Record,
            Replay
        }

        public class RecordConstraint
        {
            public enum ConstraintType
            {
                Time,
                Amount
            }

            public ConstraintType Type { get; set; }
            public int Amount { get; set; }
            public TimeSpan Length { get; set; }

        }

        public enum TrafficArgsProcessStatus
        {
            NoArguments,
            NotEnoughArguments,
            InvalidMode,
            ValidConfig
        }

        public static OptionSet InitOptionsSetObject(TrafficToolConfiguration config = null)
        {
            var options = new OptionSet();
            options.OnWarning += s => WriteLineWithColor(ConsoleColor.Yellow, s);
            options.Add("traceSeconds:", OptionCategory.TrafficRecordReplay, "Time to perform the traffic watch(seconds)", x =>
            {
                var durationConstraint = int.Parse(x);
                config.DurationConstraint = TimeSpan.FromSeconds(durationConstraint);
            });

            options.Add("traceRequests:", OptionCategory.TrafficRecordReplay, "Time to perform the traffic watch", x =>
            {
                var amountConstraint = int.Parse(x);
                config.AmountConstraint = amountConstraint;
            });
            options.Add("compressed", OptionCategory.TrafficRecordReplay, "Work with compressed json outpu/input", x => { config.IsCompressed = true; });
            options.Add("noOutput", OptionCategory.TrafficRecordReplay, "Suppress console progress output", value => config.PrintOutput = false);
            options.Add("timeout:", OptionCategory.TrafficRecordReplay, "The timeout to use for requests(seconds)", s => config.Timeout = TimeSpan.FromSeconds(int.Parse(s)));
            options.Add("key|api-key|apikey:", OptionCategory.TrafficRecordReplay, "The API-key to use, when using OAuth.", value => config.ApiKey = value);
            return options;
        }

        public static void WriteLineWithColor(ConsoleColor color, string message, params object[] args)
        {
            var previousColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message, args);
            Console.ForegroundColor = previousColor;
        }

        public static TrafficArgsProcessStatus ProcessArgs(string[] args, out TrafficToolConfiguration config)
        {
            if (args.Length == 0)
            {
                config = null;
                return TrafficArgsProcessStatus.NoArguments;
            }
            if (args.Length < 4)
            {
                config = null;
                return TrafficArgsProcessStatus.NoArguments;
            }

            try
            {
                // ReSharper disable once ObjectCreationAsStatement
                new Uri(args[1]);
            }
            catch (UriFormatException)
            {
                Console.WriteLine("ERROR : Server's url provided isn't in valid format");
                throw;
            }

            config = new TrafficToolConfiguration();

            switch (args[0])
            {
                case "rec":
                    config.Mode = TrafficToolMode.Record;
                    break;
                case "play":
                    config.Mode = TrafficToolMode.Replay;
                    break;
                default:
                    config = null;
                    return TrafficArgsProcessStatus.InvalidMode;
            }

            config.ConnectionString.Urls = new List<string> { args[1] };
            config.ConnectionString.Database = args[2];
            config.ResourceName = args[2];
            config.RecordFilePath = args[3];
            InitOptionsSetObject(config).Parse(args);

            if (config.AmountConstraint.HasValue == false && config.DurationConstraint.HasValue == false)
            {
                config.AmountConstraint = 1000;
                config.DurationConstraint = TimeSpan.FromSeconds(60);
            }

            return TrafficArgsProcessStatus.ValidConfig;
        }
    }
}
