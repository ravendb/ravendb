using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.FileSystem;
using System.Threading.Tasks;

namespace Raven.Smuggler
{
    public abstract class SmugglerOperationDispatcher<T> where T : SmugglerOptions
    {
        public async Task Execute(SmugglerAction action, T options)
        {
            try
            {
                switch (action)
                {
                    case SmugglerAction.Import:
                        await PerformImportAsync(options);
                        break;
                    case SmugglerAction.Export:
                        await PerformExportAsync(options);
                        break;
                    case SmugglerAction.Between:
                        await PerformBetweenAsync(options);
                        break;
                }
            }
            catch (AggregateException ex)
            {
                var exception = ex.ExtractSingleInnerException();
                var e = exception as WebException;
                if (e != null)
                {
                    if (e.Status == WebExceptionStatus.ConnectFailure)
                    {
                        Console.WriteLine("Error: {0} {1}", e.Message, options.Source.Url + (action == SmugglerAction.Between ? " => " + options.Destination.Url : ""));
                        var socketException = e.InnerException as SocketException;
                        if (socketException != null)
                        {
                            Console.WriteLine("Details: {0}", socketException.Message);
                            Console.WriteLine("Socket Error Code: {0}", socketException.SocketErrorCode);
                        }

                        Environment.Exit((int)e.Status);
                    }

                    var httpWebResponse = e.Response as HttpWebResponse;
                    if (httpWebResponse == null)
                        throw;

                    Console.WriteLine("Error: " + e.Message);
                    Console.WriteLine("Http Status Code: " + httpWebResponse.StatusCode + " " + httpWebResponse.StatusDescription);

                    using (var reader = new StreamReader(httpWebResponse.GetResponseStream()))
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            Console.WriteLine(line);
                        }
                    }

                    Environment.Exit((int)httpWebResponse.StatusCode);
                }
                else
                {
                    if (exception is SmugglerException)
                    {
                        Console.WriteLine(exception.Message);
                    }
                    else
                    {
                        Console.WriteLine(exception);
                    }

                    Environment.Exit(-1);
                }
            }
        }

        protected abstract Task PerformImportAsync(T parameters);
        protected abstract Task PerformExportAsync(T parameters);
        protected abstract Task PerformBetweenAsync(T parameters);
    }
}
