using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using ICSharpCode.SharpZipLib.Zip;
using Rhino.DivanDB.Server;

namespace Rhino.DivanDB.Scenarios
{
    public class Scenario
    {
        private readonly string file;
        private int responseNumber;

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
                using (new DivanServer(tempFileName, 55080))
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
                                new StreamReader(zipFile.GetInputStream(pair.Response)).ReadToEnd()
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



        private void TestSingleRequest(string request, string expectedResponse)
        {
            string actual;
            int count = 0;
            do
            {
                using (var client = new TcpClient("localhost", 55080))
                using (var stream = client.GetStream())
                {

                    var sw = new StreamWriter(stream);
                    var sr = new StreamReader(stream);

                    sw.Write(request);
                    sw.Flush();
                    stream.Flush();

                    actual = sr.ReadToEnd();
                }
                count++;
            } while (IsStaleResponse(actual) && count < 5);

            CompareResponses(
                responseNumber++,
                expectedResponse,
                actual,
                request);
        }

        private bool IsStaleResponse(string response)
        {
            return response.Contains("\"IsStale\":true");
        }

        private static void CompareResponses(int responseNumber, string expectedFull, string actualFull, string request)
        {
            var expected = expectedFull.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
            var actual = actualFull.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

            if (expected.Length != actual.Length)
            {
                throw new InvalidOperationException("Response #" +responseNumber + " doesn't match.\r\nExpected: " + expectedFull +
                                                    "\r\nActual: " + actualFull);

            }

            for (int i = 0; i < actual.Length; i++)
            {
                if ((expected[i].StartsWith("Date:") && actual[i].StartsWith("Date:") == false) ||
                    (expected[i].StartsWith("Date:") == false && expected[i] != actual[i]))
                {
                    string message = string.Format("Line {0} doesn't match in response!\r\nRequest is:\r\n{5}\r\nExpected: {1}\r\nActual: {2}\r\nExpected full: \r\n{3}\r\nActual full: \r\n{4}",
                                                   i, expected[i], actual[i], expectedFull, actualFull,request);
                    throw new InvalidOperationException(message);
                }
            }
        }

        private static int OrderFromRequestName(string file)
        {
            string number = Path.GetFileNameWithoutExtension(file).Split('_').First();
            int result;
            if (int.TryParse(number, out result) == false)
                throw new InvalidOperationException("Could not extract order from: " + file);
            return result;
        }
    }
}