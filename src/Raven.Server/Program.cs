using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Hosting;
using Microsoft.AspNet.Hosting.Internal;
using Microsoft.AspNet.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server
{
    public class Program
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

        public static int Main(string[] args)
        {
            WelcomeMessage.Print();

            var configuration = new RavenConfiguration();
            if (args != null)
            {
                configuration.AddCommandLine(args);
            }
            configuration.Initialize();

            using (var server = new RavenServer(configuration))
            {
                try
                {
                    server.Initialize();

                    Console.WriteLine("Listening to: " + configuration.WebHostConfig.Get<string>("server.urls"));
                    Console.WriteLine("Server started, listening to requests...");

                    //TODO: Move the command line options to here
                    while (true)
                    {
                        if (Console.ReadLine() == "q")
                            break;

                        // Console.ForegroundColor++;
                    }
                    Log.Info("Server is shutting down");
                    return 0;
                }
                catch (Exception e)
                {
                    Log.FatalException("Failed to initialize the server", e);
                    Console.WriteLine(e);
                    return -1;
                }
            }
        }
    }
}