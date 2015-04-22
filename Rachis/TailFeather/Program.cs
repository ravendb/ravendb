using System;
using System.IO;
using System.Web.Http;
using CommandLine;
using CommandLine.Text;
using Microsoft.Owin.Hosting;
using Owin;
using Rachis;
using Rachis.Storage;
using Rachis.Transport;
using TailFeather.Storage;
using Voron;

namespace TailFeather
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			var options = new TailFeatherCommandLineOptions();
			if (Parser.Default.ParseArguments(args, options) == false)
			{
				var autoBuild = HelpText.AutoBuild(options);
				HelpText.DefaultParsingErrorsHandler(options, autoBuild);
				Console.WriteLine(autoBuild.ToString());
				return;
			}

			var nodeName = options.NodeName ?? (Environment.MachineName + ":" + options.Port);
			Console.Title = string.Format("Node name: {0}, port: {1}", nodeName, options.Port);

			var kvso = StorageEnvironmentOptions.ForPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, options.DataPath, "KeyValue"));
			using (var statemachine = new KeyValueStateMachine(kvso))
			{
				var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, options.DataPath, "Raft"));
				var httpTransport = new HttpTransport(nodeName);
				var raftEngineOptions = new RaftEngineOptions(
					new NodeConnectionInfo
					{
						Name = nodeName,
						Uri = new Uri("http://" + Environment.MachineName + ":" + options.Port),
					},
					storageEnvironmentOptions,
					httpTransport,
					statemachine
					)
				{
					ElectionTimeout = 5 * 1000,
					HeartbeatTimeout = 1000,
					MaxLogLengthBeforeCompaction = 25
				};

				if (options.Boostrap)
				{
					PersistentState.ClusterBootstrap(raftEngineOptions);
					Console.WriteLine("Setup node as the cluster seed, exiting...");
					return;
				}

				using (var raftEngine = new RaftEngine(raftEngineOptions))
				{
					using (WebApp.Start(new StartOptions
					{
						Urls = { "http://+:" + options.Port + "/" }
					}, builder =>
					{
						var httpConfiguration = new HttpConfiguration();
						httpConfiguration.Formatters.Remove(httpConfiguration.Formatters.XmlFormatter);
						httpConfiguration.Formatters.JsonFormatter.SerializerSettings.Formatting = Newtonsoft.Json.Formatting.Indented;
						httpConfiguration.Formatters.JsonFormatter.SerializerSettings.Converters.Add(new Newtonsoft.Json.Converters.StringEnumConverter());
						RaftWebApiConfig.Load();
						httpConfiguration.MapHttpAttributeRoutes();
						httpConfiguration.Properties[typeof(HttpTransportBus)] = httpTransport.Bus;
						httpConfiguration.Properties[typeof(RaftEngine)] = raftEngine;
						builder.UseWebApi(httpConfiguration);
					}))
					{
						Console.WriteLine("Ready @ http://" + Environment.MachineName + ":" + options.Port + "/, press ENTER to stop");

						Console.ReadLine();
					}
				}
			}
		}
	}
}