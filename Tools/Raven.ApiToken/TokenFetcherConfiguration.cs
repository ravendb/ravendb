// -----------------------------------------------------------------------
//  <copyright file="TokenFetcherConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Security.Policy;

namespace Raven.ApiToken
{
    public class TokenFetcherConfiguration
    {
        public string ServerUrl { get; set; }
        public string ApiKey { get; set; }


        public enum ProcessStatus
        {
            NoArguments,
            NotEnoughArguments,
            ValidConfig
        }

        public static ProcessStatus ProcessArgs(string[] args, out TokenFetcherConfiguration config)
        {
            if (args.Length == 0)
            {
                config = null;
                return ProcessStatus.NoArguments;
            }

            if (args.Length < 2)
            {
                config = null;
                return ProcessStatus.NotEnoughArguments;
            }

            // ReSharper disable once ObjectCreationAsStatement
            new Url(args[0]); // validate by creatin Url instance. 

            config = new TokenFetcherConfiguration
            {
                ServerUrl = args[0],
                ApiKey = args[1]
            };

            return ProcessStatus.ValidConfig;
        }
    }
}