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
            using (var client = new TcpClient())
            {
                client.Connect("localhost", 55080);
                using (var stream = client.GetStream())
                {
                    var sw = new StreamWriter(stream);
                    var sr = new StreamReader(stream);

                    var request = File.ReadAllText(requestFile);
                    sw.Write(request);
                    sw.Flush();
                    stream.Flush();

                    string actual = sr.ReadToEnd();
                    string expected = File.ReadAllText(responseFile);

                    CompareResponses(
                        responseFile,
                        expected,
                        actual,
                        expected.Split(new[] {Environment.NewLine}, StringSplitOptions.None),
                        actual.Split(new[] {Environment.NewLine}, StringSplitOptions.None)
                        );
                }
            }
        }

        private static void CompareResponses(string file, string expectedFull, string actualFull, string[] expected, string[] actual)
        {
            if (expected.Length != actual.Length)
            {
                throw new InvalidOperationException(file + " doesn't match.\r\nExpected: " + expectedFull +
                                                    "\r\nActual: " + actualFull);

            }

            for (int i = 0; i < actual.Length; i++)
            {
                if (expected[i].StartsWith("Date:"))
                {
                    if (actual[i].StartsWith("Date:") == false)
                        throw new InvalidOperationException("Line " + i + " doesn't match for " + file + "\r\nExpected: " +
                                                          expected[i] +
                                                          "\r\nActual: " + actual[i] + "\r\nExpected full: \r\n" +
                                                          expectedFull + "\r\nActual full: \r\n" + actualFull);

                }
                else if (expected[i] != actual[i])
                {
                    throw new InvalidOperationException("Line " + i + " doesn't match for " + file + "\r\nExpected: " +
                                                        expected[i] +
                                                        "\r\nActual: " + actual[i] + "\r\nExpected full: \r\n" +
                                                        expectedFull + "\r\nActual full: \r\n" + actualFull);
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