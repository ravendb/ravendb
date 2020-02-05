using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Nancy.Hosting.Kestrel;
using Redbus;
using Redbus.Interfaces;

namespace Tests.ResourceSnapshotAggregator
{
    /*
     * Note:
     * <=> On Windows, this can be registered as a service by using the following powershell command (requires admin):
     * sc.exe create ResourceSnapshotAggregator binPath=[path]\Tests.ResourceSnapshotAggregator.exe     
     * <=> On Linux, this can be setup as systemd service
     */
    public class Program
    {
        private static readonly IEventBus _messageBus = new EventBus();

        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }
        
        internal class Startup
        {
            public void Configure(IApplicationBuilder app) => 
                app.UseNancy(opt => opt.Bootstrapper = new NancyBootstrapper(_messageBus));
        }

        public static IHostBuilder CreateHostBuilder(string[] args)
        {

            return Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .UseSystemd()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddHostedService<Worker>();

                    var configuration = hostContext.Configuration;
                    var settings = configuration.GetSection("Servers")
                                                .Get<ServiceSettings>();

                    //TODO: add configuration support for kestrel listener
                    var webHost = WebHost
                        .CreateDefaultBuilder(args)
                        .UseKestrel(cfg =>
                        {
                            cfg.AllowSynchronousIO = true;
                            cfg.ListenAnyIP(settings.NotificationListenerPort);
                        })
                        .UseStartup<Startup>()
                        .Build();
                    
                    services.AddSingleton(settings);
                    services.AddSingleton(webHost);
                    services.AddSingleton(_messageBus);

                });
        }
    }

  
}
