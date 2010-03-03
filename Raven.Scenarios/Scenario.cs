using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using ICSharpCode.SharpZipLib.Zip;
using Raven.Database.Extensions;
using Raven.Server;
using Raven.Server.Responders;

namespace Raven.Scenarios
{
    public class Scenario
    {
        private readonly string file;
        private int responseNumber;
        const int testPort = 58080;
        private string lastEtag;
        private readonly Regex etagFinder = new Regex(@",""expectedETag"":""(\{{0,1}([0-9a-fA-F]){8}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){4}-([0-9a-fA-F]){12}\}{0,1})"",");

        public Scenario(string file)
        {
            this.file = file;
        }

        public void Execute()
        {
            string tempFileName = Path.GetTempFileName();
            File.Delete(tempFileName);
            try
            {
                DivanServer.EnsureCanListenToWhenInNonAdminContext(testPort);
                using (new DivanServer(tempFileName, testPort))
                {
                    using (var zipFile = new ZipFile(file))
                    {
                        var zipEntries = zipFile.OfType<ZipEntry>()
                            .Where(x => x.Name.StartsWith("raw/"))
                            .Where(x => Path.GetExtension(x.Name) == ".txt")
                            .GroupBy(x => x.Name.Split('_').First())
                            .Select(x => new { Request = x.First(), Response = x.Last() })
                            .ToArray();

                        foreach (var pair in zipEntries)
                        {
                            TestSingleRequest(
                                new StreamReader(zipFile.GetInputStream(pair.Request)).ReadToEnd(),
                                zipFile.GetInputStream(pair.Response).ReadData()
                                );
                        }
                    }
                }
            }
            finally
            {
                Directory.Delete(tempFileName, true);
            }
        }



        private void TestSingleRequest(string request, byte[] expectedResponse)
        {
            Tuple<string, NameValueCollection, string> actual;
            int count = 0;
            do
            {
                actual = SendRequest(request);
                count++;
                if (!IsStaleResponse(actual.First))
                    break;
                Thread.Sleep(100);
            } while (count < 5);

            actual.First = etagFinder.Replace(actual.First, "$`" + lastEtag + "$'");

            lastEtag = actual.Second["ETag"];
            CompareResponses(
                responseNumber++,
                expectedResponse,
                actual,
                request);
        }

        private Tuple<string, NameValueCollection, string> SendRequest(string request)
        {
            using (var sr = new StringReader(request))
            {
                string[] reqParts = sr.ReadLine().Split(' ');
                var req = (HttpWebRequest)WebRequest.Create(reqParts[1].Replace(":8080/", ":" + testPort + "/"));
                req.Method = reqParts[0];

                string header;
                while (string.IsNullOrEmpty((header = sr.ReadLine())) == false)
                {
                    string[] headerParts = header.Split(new[] { ": " }, 2, StringSplitOptions.None);
                    if (new[] { "Host", "Content-Length", "User-Agent" }.Any(s => s.Equals(headerParts[0], StringComparison.InvariantCultureIgnoreCase)))
                        continue;
                    if (headerParts[0] == "ETag")
                        headerParts[1] = lastEtag;
                    req.Headers[headerParts[0]] = headerParts[1];
                }

                if (req.Method != "GET")
                {
                    using (var requestStream = req.GetRequestStream())
                    using (var writer = new StreamWriter(requestStream))
                    {
                        writer.Write(sr.ReadToEnd());
                        writer.Flush();
                    }
                }

                var webResponse = GetResponse(req);
                {
                    return new Tuple<string, NameValueCollection, string>
                    {
                        First = new StreamReader(webResponse.GetResponseStream()).ReadToEnd(),
                        Second = webResponse.Headers,
                        Third =
                            "HTTP/" + webResponse.ProtocolVersion + " " + (int)webResponse.StatusCode + " " +
                            webResponse.StatusDescription
                    };
                }
            }
        }

        private static HttpWebResponse GetResponse(HttpWebRequest req)
        {
            HttpWebResponse webResponse;
            try
            {
                webResponse = (HttpWebResponse)req.GetResponse();
            }
            catch (WebException e)
            {
                webResponse = (HttpWebResponse)e.Response;
            }
            return webResponse;
        }

        private static bool IsStaleResponse(string response)
        {
            return response.Contains("\"IsStale\":true");
        }

        private static void CompareResponses(int responseNumber, byte[] response, Tuple<string, NameValueCollection, string> actual, string request)
        {
            var sr = new StringReader(HandleChunking(response));
            string statusLine = sr.ReadLine();
            if (statusLine != actual.Third)
            {
                throw new InvalidDataException(
                    string.Format("Request {0} status differs. Expected {1}, Actual {2}\r\nRequest:\r\n{3}",
                                  responseNumber, statusLine, actual.Third, request));
            }
            string header;
            while (string.IsNullOrEmpty((header = sr.ReadLine())) == false)
            {
                string[] parts = header.Split(new[] { ": " }, 2, StringSplitOptions.None);
                if (parts[0] == "Date" || parts[0] == "Content-Length" || 
                    parts[0] == "ETag")
                    continue;
                if (actual.Second[parts[0]] != parts[1])
                {
                    throw new InvalidDataException(
                        string.Format("Request {0} header {1} differs. Expected {2}, Actual {3}\r\nRequest:\r\n{4}",
                                      responseNumber, parts[0], parts[1], actual.Second[parts[0]], request));
                }
            }

            string expectedLine;
            var rr = new StringReader(actual.First);
            int line = 0;
            while (string.IsNullOrEmpty((expectedLine = sr.ReadLine())) == false)
            {
                line++;
                string actualLine = rr.ReadLine();
                if (expectedLine != actualLine)
                {
                    throw new InvalidDataException(
                        string.Format("Request {0} line {1} differs. Expected\r\n{2}\r\nActual\r\n{3}\r\nRequest{4}",
                                      responseNumber, line, expectedLine, actualLine, request));
                }
            }
        }

        private static string HandleChunking(byte[] data)
        {
            var memoryStream = new MemoryStream(data);
            var streamReader = new StreamReader(memoryStream);

            var sb = new StringBuilder();
            sb.AppendLine(streamReader.ReadLine());//status
            string line;
            while ((line = streamReader.ReadLine()) != "")
                sb.AppendLine(line);// header
            sb.AppendLine();//separator line
            if (sb.ToString().Contains("Transfer-Encoding: chunked") == false)
            {
                sb.Append(streamReader.ReadToEnd());
                return sb.ToString();
            }

            string chunk;
            while (((chunk = ReadChuck(memoryStream))) != null)
            {
                sb.Append(chunk);
            }
            return sb.ToString();
        }

        private static string ReadChuck(MemoryStream memoryStream)
        {
            var chunkSizeBytes = new List<byte>();
            byte cur;
            do
            {
                int readByte = memoryStream.ReadByte();
                if (readByte == -1)
                    return null;
                cur = (byte)readByte;
            } while (cur != '\n' && chunkSizeBytes.LastOrDefault() != '\r');
            chunkSizeBytes.RemoveAt(chunkSizeBytes.Count - 1);
            int size = int.Parse(Encoding.UTF8.GetString(chunkSizeBytes.ToArray()));

            var buffer = new byte[size];
            memoryStream.Read(buffer, 0, size);//not doing repeated read because it is all in mem
            return Encoding.UTF8.GetString(buffer);
        }
    }
}