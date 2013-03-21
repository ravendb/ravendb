//-----------------------------------------------------------------------
// <copyright file="RssDictionarySample.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace RssDictionarySample
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Xml.Linq;
    using Microsoft.Isam.Esent.Collections.Generic;

    /// <summary>
    /// Contains information about the RSS feed.
    /// </summary>
    internal struct RssFeedData
    {
        /// <summary>
        /// Gets or sets the (UTC) time that this feed was last successfully
        /// retrieved. This value is initially null.
        /// </summary>
        public DateTime? LastRetrieved { get; set; }

        /// <summary>
        /// Gets or sets the RSS feed data.
        /// </summary>
        public string Data { get; set; }
    }

    /// <summary>
    /// An extremely simple RSS reader. This keeps a URL => data mapping
    /// in a persistent dictionary and supports updates, adding a URL 
    /// and printing a summary of the RSS entries.
    /// </summary>
    public static class TrivialRssReader
    {
        /// <summary>
        /// A dictionary that maps URL => content. This dictionary is persisted,
        /// with all the files in the 'RssDictionary' subdirectory. If the dictionary
        /// doesn't exist the constructor will create it, otherwise the existing
        /// dictionary will be opened. There should be only one dictionary object
        /// per file (directory) but the dictionary can be used by multiple threads.
        /// </summary>
        private static readonly PersistentDictionary<string, RssFeedData> dictionary =
            new PersistentDictionary<string, RssFeedData>("RssDictionary");

        /// <summary>
        /// A trivial RSS reader with local caching. 
        /// </summary>
        /// <param name="args">
        /// No arguments: print the RSS. '-update': update all the RSS feeds in the
        /// dictionary. Otherwise add all arguments to the dictionary as URLs.
        /// </param>
        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Print();
            }
            else if ("-update" == args[0])
            {
                Update();
            }
            else
            {
                Add(args);
            }
        }

        /// <summary>
        /// Add the specified urls to the RSS dictionary.
        /// </summary>
        /// <param name="urls">Urls to add.</param>
        private static void Add(string[] urls)
        {
            foreach (string url in urls)
            {
                // Don't overwrite an existing entry
                if (!dictionary.ContainsKey(url))
                {
                    dictionary[url] = new RssFeedData
                    {
                        LastRetrieved = null,
                        Data = String.Empty,
                    };
                }
            }
        }

        /// <summary>
        /// Get the latest RSS for all the URLs in the dictionary. 
        /// </summary>
        private static void Update()
        {
            foreach (string url in dictionary.Keys)
            {
                // For simplicity, so this synchronously, single-threaded and
                // without error handling.
                var response = new StreamReader(WebRequest.Create(url).GetResponse().GetResponseStream());
                dictionary[url] = new RssFeedData
                {
                    LastRetrieved = DateTime.UtcNow,
                    Data = response.ReadToEnd(),
                };
            }
        }

        /// <summary>
        /// Print all the RSS data in the dictionary.
        /// </summary>
        private static void Print()
        {
            foreach (string url in dictionary.Keys)
            {
                ParseAndPrintRssData(dictionary[url].Data);
            }
        }

        /// <summary>
        /// Parse and print some RSS data. This doesn't do a particularily
        /// good job of parsing/printing.
        /// </summary>
        /// <param name="rssdata">The data to print.</param>
        private static void ParseAndPrintRssData(string rssdata)
        {
            var separator = new string('-', 80);
            XDocument xdoc = XDocument.Parse(rssdata);
            var feeds = from item in xdoc.Descendants("item")
                         select new
                         {
                             Title = (string)item.Element("title"),
                             Date = (string)item.Element("pubDate"),
                             Url = (string)item.Element("link"),
                         };
            foreach (var item in feeds)
            {
                Console.WriteLine("DATE: {0}", item.Date);
                Console.WriteLine("TITLE: {0}", item.Title);
                Console.WriteLine("URL: {0}", item.Url);
                Console.WriteLine(separator);
            }

            Console.WriteLine();
        }
    }
}
