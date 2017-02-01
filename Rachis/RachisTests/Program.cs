using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rachis;
using Rachis.Commands;
using Rachis.Communication;
using Rachis.Messages;
using Rachis.Storage;

namespace RachisTests
{
    public class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                mre = new ManualResetEvent(false);
                var followerNodeConnection = new NodeConnectionInfo
                {
                    Name = "Foo",
                    IsNoneVoter = false,
                    ConnectFunc = (s) => TcpConnector.Connect(s, "Foo")
                };
                followeRaftEngine = new RaftEngine(new RaftEngineOptions(followerNodeConnection ,new DictionaryStateMachine()));
                var leaderNodeConnection = new NodeConnectionInfo
                {
                    Name = "Bar",
                    IsNoneVoter = false,
                    ConnectFunc = (s) => TcpConnector.Connect(s, "Bar")
                };
                var leaderOptions = new RaftEngineOptions(leaderNodeConnection, new DictionaryStateMachine());
                
                leadeRaftEngine = new RaftEngine(leaderOptions, new Topology(new Guid(), new[] { leaderOptions.SelfConnectionInfo }, Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>()));
                //setting up the communication
                TcpConnector.IdsToRaftEngines = new Dictionary<string, RaftEngine> { { "Foo", followeRaftEngine }, { "Bar", leadeRaftEngine } };
                leadeRaftEngine.AddToCluster(followerNodeConnection);
                leadeRaftEngine.AppendCommand(new DictionaryCommand.Set { Key = "Foo", Value = 13 });
                leadeRaftEngine.AppendCommand(new DictionaryCommand.Set { Key = "bluh", Value = 7 });
                leadeRaftEngine.AppendCommand(new DictionaryCommand.Set { Key = "po", Value = 4 });
                leadeRaftEngine.AppendCommand(new DictionaryCommand.Inc { Key = "po", Value = 2 });

                Task.Factory.StartNew(() =>
                {
                while (true)
                {
                    int po;
                        (followeRaftEngine.StateMachine as DictionaryStateMachine).Data.TryGetValue("po", out po);
                        if (po == 6)
                            mre.Set();
                    }                    
                });
                mre.WaitOne();
                Console.WriteLine("We are done!");
                
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

        }

        private static RaftEngine followeRaftEngine, leadeRaftEngine;
        private static string fileName;
        private static ManualResetEvent mre;
  /*      private static void MimicLeaderBehavior()
        {
            using (var fw = File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                List<LogEntry> entries = new List<LogEntry>();
                var append = new AppendEntries
                {
                    Entries = entries.ToArray(),
                    LeaderCommit = 0,
                    PositionOfTopologyChange = -1,
                    PrevLogIndex = 0,
                    PrevLogTerm = 0,
                    Term = 1
                };
                followeRaftEngine.StartTalkingToFollower(append);
                Thread.Sleep(5 * 1000);                
                byte[] buffer = new byte[1024];
                ReadResponse(fw, buffer);
                entries = new List<LogEntry>();
                entries.Add(new LogEntry
                {
                    Data = new DictionaryCommand.Set { Key = "Foo", Value = 7 }.ToBytes(),
                    Index = 1,
                    IsTopologyChange = false,
                    Term = 1
                });

                append = new AppendEntries
                {
                    Entries = entries.ToArray(),
                    LeaderCommit = 0,
                    PositionOfTopologyChange = -1,
                    PrevLogIndex = 0,
                    PrevLogTerm = 0,
                    Term = 1
                };
                WriteCommandAndReadResponse(fw, buffer, append);
                entries = new List<LogEntry>();
                entries.Add(new LogEntry
                {
                    Data = new DictionaryCommand.Set { Key = "Bar", Value = 13 }.ToBytes(),
                    Index = 2,
                    IsTopologyChange = false,
                    Term = 1
                });

                append = new AppendEntries
                {
                    Entries = entries.ToArray(),
                    LeaderCommit = 1,
                    PositionOfTopologyChange = -1,
                    PrevLogIndex = 1,
                    PrevLogTerm = 1,
                    Term = 1
                };
                WriteCommandAndReadResponse(fw, buffer, append);
                Thread.Sleep(5*1000);
                mre.Set();
            }
        }
*/
        private static void WriteCommandAndReadResponse(FileStream fw, byte[] buffer,AppendEntries append)
        {

            var bytes = System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(append));
            fw.Write(BitConverter.GetBytes(bytes.Length), 0, 4);
            fw.Write(bytes, 0, bytes.Length);
            fw.Flush();
            Thread.Sleep(5*1000);
            ReadResponse(fw, buffer);
        }

        private static AppendEntriesResponse ReadResponse(Stream fw, byte[] buffer)
        {
            int read;
            int totoalRead = 0;
            do
            {
                read = fw.Read(buffer, totoalRead, 100);
                totoalRead += read;
            } while (read > 0);
            var size = BitConverter.ToInt32(buffer, 0);
            var responseStr = System.Text.Encoding.UTF8.GetString(buffer, 4, size);
            Console.WriteLine(responseStr);
            return JsonConvert.DeserializeObject<AppendEntriesResponse>(responseStr);
        }
    }
}
