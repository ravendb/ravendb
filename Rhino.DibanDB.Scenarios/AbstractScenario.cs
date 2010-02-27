using System;
using System.IO;
using System.Net.Sockets;
using Rhino.DivanDB.Server;
using Xunit;
using System.Linq;

namespace Rhino.DibanDB.Scenarios
{
    public abstract class AbstractScenario
    {
        public string Directory
        {
            get { return @"C:\Work\rhino-divandb\Rhino.DibanDB.Scenarios\" + GetType().Namespace.Split('.').Last(); }
        }

        [Fact]
        public void Execute()
        {
            string tempFileName = Path.GetTempFileName();
            File.Delete(tempFileName);
            try
            {
                using (new DivanServer(tempFileName, 55080))
                {
                    foreach (var requestFile in System.IO.Directory.GetFiles(Directory, "*.request")
                        .OrderBy(file => OrderFromRequestName(file)))
                    {
                        string responseFile = Path.Combine(Path.GetDirectoryName(requestFile),
                                                           Path.GetFileNameWithoutExtension(requestFile) + ".response");
                        if (File.Exists(responseFile) == false)
                        {
                            throw new InvalidOperationException("Cannot find matching response file " + responseFile);
                        }
                        TestSingleRequest(requestFile, responseFile);

                    }
                }
            }
            finally
            {
                System.IO.Directory.Delete(tempFileName, true);
            }
        }

        private void TestSingleRequest(string requestFile, string responseFile)
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

                    var request = File.ReadAllText(requestFile);
                    sw.Write(request);
                    sw.Flush();
                    stream.Flush();

                    actual = sr.ReadToEnd();
                }
                count++;
            } while (IsStaleResponse(actual) == false && count < 5);

            string expected = File.ReadAllText(responseFile);

            CompareResponses(
                responseFile,
                expected,
                actual);
        }

        private bool IsStaleResponse(string response)
        {
            return response.Contains("\"IsStale\":true");
        }

        private static void CompareResponses(string file, string expectedFull, string actualFull)
        {
            var expected = expectedFull.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
            var actual = actualFull.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

            if (expected.Length != actual.Length)
            {
                throw new InvalidOperationException(file + " doesn't match.\r\nExpected: " + expectedFull +
                                                    "\r\nActual: " + actualFull);

            }

            for (int i = 0; i < actual.Length; i++)
            {
                if ((expected[i].StartsWith("Date:") && actual[i].StartsWith("Date:") == false) ||
                    (expected[i].StartsWith("Date:") == false && expected[i] != actual[i]))
                {
                    string message = string.Format("Line {0} doesn't match for {1}\r\nExpected: {2}\r\nActual: {3}\r\nExpected full: \r\n{4}\r\nActual full: \r\n{5}",
                        i, file, expected[i], actual[i], expectedFull, actualFull);
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