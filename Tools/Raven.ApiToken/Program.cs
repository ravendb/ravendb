using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.OAuth;

namespace Raven.ApiToken
{
    class Program
    {
        static void Main(string[] args)
        {
            TokenFetcherConfiguration config;
            var parseStatus = TokenFetcherConfiguration.ProcessArgs(args, out config);

            switch (parseStatus)
            {
                case TokenFetcherConfiguration.ProcessStatus.NoArguments:
                    PrintUsage();
                    break;
                case TokenFetcherConfiguration.ProcessStatus.NotEnoughArguments:
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Illegal arguments amount, see usage instructions:");
                    Console.ForegroundColor = ConsoleColor.White;
                    break;
                case TokenFetcherConfiguration.ProcessStatus.ValidConfig:
                    var token = TryObtainAuthToken(config).Result;
                    Console.WriteLine(token);
                    break;
            }
        }

        private static async Task<string> TryObtainAuthToken(TokenFetcherConfiguration config)
        {
            var securedAuthenticator = new SecuredAuthenticator(autoRefreshToken: false);
            var result = await securedAuthenticator.DoOAuthRequestAsync(null, config.ServerUrl + "/OAuth/API-Key", config.ApiKey);
            using (var httpClient = new HttpClient())
            {
                result(httpClient);
                var authenticationHeaderValue = httpClient.DefaultRequestHeaders.Authorization;
                return authenticationHeaderValue.Parameter;
            }
        }

        private static void PrintUsage()
        {
            Console.ForegroundColor = ConsoleColor.DarkMagenta;
            Console.WriteLine(@"
ApiKey token generation utility for RavenDB
----------------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------------", SystemTime.UtcNow.Year);
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(@"
Usage:
    Raven.ApiToken [Url] [ApiKey]

Example:
    Raven.ApiToken http://localhost:8080/ ""key1/sAdVA0KLqigQu67Dxj7a""");
        }
    }
}
