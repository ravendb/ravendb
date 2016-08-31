// -----------------------------------------------------------------------
//  <copyright file="TrafficRecorderConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using NDesk.Options;
using Raven.Abstractions.Data;

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

        private static NetworkCredential GetCredentials(RavenConnectionStringOptions connectionStringOptions)
        {
            var cred = connectionStringOptions.Credentials as NetworkCredential;
            if (cred != null)
                return cred;
            cred = new NetworkCredential();
            connectionStringOptions.Credentials = cred;
            return cred;
        }

        public static OptionSet InitOptionsSetObject(TrafficToolConfiguration config = null)
        {
            var options = new OptionSet();
            options.OnWarning += s => WriteLineWithColor(ConsoleColor.Yellow, s);
            options.Add("traceSeconds:", OptionCategory.TrafficRecordReplay, "Time to perform the traffic watch(seconds)", x =>
            {
                var durationConstraint = Int32.Parse(x);
                config.DurationConstraint = TimeSpan.FromSeconds(durationConstraint);
            });

            options.Add("traceRequests:", OptionCategory.TrafficRecordReplay, "Time to perform the traffic watch", x =>
            {
                var amountConstraint = Int32.Parse(x);
                config.AmountConstraint = amountConstraint;
            });
            options.Add("compressed", OptionCategory.TrafficRecordReplay, "Work with compressed json outpu/input", x => { config.IsCompressed = true; });
            options.Add("noOutput", OptionCategory.TrafficRecordReplay, "Suppress console progress output", value => config.PrintOutput = false);
            options.Add("timeout:", OptionCategory.TrafficRecordReplay, "The timeout to use for requests(seconds)", s => config.Timeout = TimeSpan.FromSeconds(int.Parse(s)));
            options.Add("u|user|username:", OptionCategory.TrafficRecordReplay, "The username to use when the database requires the client to authenticate.", value => GetCredentials(config.ConnectionString).UserName = value);
            options.Add("p|pass|password:", OptionCategory.TrafficRecordReplay, "The password to use when the database requires the client to authenticate.", value => GetCredentials(config.ConnectionString).Password = value);
            options.Add("domain:", OptionCategory.TrafficRecordReplay, "The domain to use when the database requires the client to authenticate.", value => GetCredentials(config.ConnectionString).Domain = value);
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
            // new Url(args[1]); // TODO :: verify args[1] as valid url
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

            config.ConnectionString.Url = args[1];
            config.ConnectionString.DefaultDatabase = args[2];
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
