using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Xml;
using Rhino.DivanDB.Server;
using Xunit.Sdk;

namespace Rhino.DivanDB.Tests.Scenarios
{
    public class ScenarioCommand : ITestCommand
    {
        private readonly string directory;
        private readonly MethodInfo methodInfo;

        public ScenarioCommand(string directory, MethodInfo methodInfo)
        {
            this.directory = directory;
            this.methodInfo = methodInfo;
        }

        public MethodResult Execute(object testClass)
        {
            string tempFileName = Path.GetTempFileName();
            File.Delete(tempFileName);
            try
            {
                using (new DivanServer(tempFileName, 55080))
                {
                    foreach (var requestFile in Directory.GetFiles(directory, "*.request")
                        .OrderBy(file => OrderFromRequestName(file)))
                    {
                        string responseFile = Path.Combine(Path.GetDirectoryName(requestFile),
                                              Path.GetFileNameWithoutExtension(requestFile) + ".response");
                        if (File.Exists(responseFile) == false)
                        {
                            throw new InvalidOperationException("Cannot find matching response file " + responseFile);
                        }
                        try
                        {
                            TestSingleRequest(requestFile, responseFile);
                        }
                        catch (Exception e)
                        {
                            return new FailedResult(methodInfo, e, Path.GetFileName(directory));
                        }
                    }
                }
            }
            finally
            {
                Directory.Delete(tempFileName, true);
            }
            return new PassedResult(methodInfo, Path.GetFileName(directory));
        }

        private void TestSingleRequest(string requestFile, string responseFile)
        {
            using (var client = new TcpClient("localhost", 55080))
            using (var stream = client.GetStream())
            {
                byte[] request = File.ReadAllBytes(requestFile);
                stream.Write(request, 0, request.Length);

                using (var sr = new StreamReader(stream))
                {
                    string actual = sr.ReadToEnd();
                    string expected = File.ReadAllText(responseFile);

                    CompareResponses(
                        responseFile,
                        expected,
                        actual,
                        expected.Split(new[] { Environment.NewLine }, StringSplitOptions.None),
                        actual.Split(new[] { Environment.NewLine }, StringSplitOptions.None)
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
                        throw new InvalidOperationException("Line " + i + " doesn't match for " + file + "\r\nExpected: " + expected[i] +
                                                            "\r\nActual: " + actual[i]);
                }
                else if (expected[i] != actual[i])
                {
                    throw new InvalidOperationException("Line " + i + " doesn't match for " + file + "\r\nExpected: " + expected[i] +
                                                        "\r\nActual: " + actual[i]);
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

        public XmlNode ToStartXml()
        {
            var doc = new XmlDocument();
            doc.LoadXml("<dummy/>");
            var testNode = XmlUtility.AddElement(doc.ChildNodes[0], "start");

            XmlUtility.AddAttribute(testNode, "name", DisplayName);
            XmlUtility.AddAttribute(testNode, "type", "scenario");
            XmlUtility.AddAttribute(testNode, "method", directory);

            return testNode;
        }

        public string DisplayName
        {
            get { return "Scenario: " + directory; }
        }

        public bool ShouldCreateInstance
        {
            get { return false; }
        }
    }
}