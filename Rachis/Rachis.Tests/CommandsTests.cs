using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rachis.Messages;
using Xunit;
using Xunit.Extensions;

namespace Rachis.Tests
{
    public class CommandsTests : RaftTestsBase
    {
        [Fact]
        public void When_command_committed_CompletionTaskSource_is_notified()
        {
            const int CommandCount = 5;
            var leader = CreateNetworkAndGetLeader(3);
            var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
                .All()
                .With(x => x.Completion = new TaskCompletionSource<object>())
                .With(x => x.AssignedIndex = -1)
                .Build()
                .ToList();


            var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
            var commitsAppliedEvent = new ManualResetEventSlim();

            nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
            {
                //CommandCount + 1 --> take into account NOP command that leader sends after election
                if (newIndex == CommandCount + 1)
                    commitsAppliedEvent.Set();
            };

            commands.ForEach(leader.AppendCommand);

            Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.Options.ElectionTimeout * 2));
            commands.Should().OnlyContain(cmd => cmd.Completion.Task.Status == TaskStatus.RanToCompletion);
        }

        //this test is a show-case of how to check for command commit time-out
        [Fact(Skip="Flaky test")]
        public void Command_not_committed_after_timeout_CompletionTaskSource_is_notified()
        {
            const int CommandCount = 5;
            var leader = CreateNetworkAndGetLeader(3);
            var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
                .All()
                .With(x => x.Completion = new TaskCompletionSource<object>())
                .With(x => x.AssignedIndex = -1)
                .Build()
                .ToList();


            var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
            var commitsAppliedEvent = new ManualResetEventSlim();

            nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
            {
                //essentially fire event for (CommandCount - 1) + Nop command
                if (newIndex == CommandCount)
                    commitsAppliedEvent.Set();
            };

            //don't append the last command yet
            commands.Take(CommandCount - 1).ToList().ForEach(leader.AppendCommand);
            //make sure commands that were appended before network leader disconnection are replicated
            Assert.True(commitsAppliedEvent.Wait(nonLeaderNode.Options.ElectionTimeout * 3));

            DisconnectNode(leader.Name);

            var lastCommand = commands.Last();
            var commandCompletionTask = lastCommand.Completion.Task;

            leader.AppendCommand(lastCommand);

            var aggregateException = Assert.Throws<AggregateException>(() => commandCompletionTask.Wait(leader.Options.ElectionTimeout * 2));
            Assert.IsType<TimeoutException>(aggregateException.InnerException);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void Leader_AppendCommand_for_first_time_should_distribute_commands_between_nodes(int nodeCount)
        {
            const int CommandCount = 5;
            var commandsToDistribute = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
                .All()
                .With(x => x.Completion = null)
                .Build()
                .ToList();

            var leader = CreateNetworkAndGetLeader(nodeCount);
            var entriesAppended = new Dictionary<string, List<LogEntry>>();
            Nodes.ToList().ForEach(node =>
            {
                entriesAppended.Add(node.Name, new List<LogEntry>());
                node.EntriesAppended += logEntries => entriesAppended[node.Name].AddRange(logEntries);
            });


            var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
            var commitsAppliedEvent = new ManualResetEventSlim();
            if (nonLeaderNode.CommitIndex == CommandCount + 1) //precaution
                commitsAppliedEvent.Set();
            nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
            {
                //CommandCount + 1 --> take into account NOP command that leader sends after election
                if (newIndex == CommandCount + 1)
                    commitsAppliedEvent.Set();
            };

            commandsToDistribute.ForEach(leader.AppendCommand);

            var millisecondsTimeout = 10000 * nodeCount;
            Assert.True(commitsAppliedEvent.Wait(millisecondsTimeout), "within " + millisecondsTimeout + " sec. non leader node should have all relevant commands committed");
        }

        [Theory]
        [InlineData(3)]
        [InlineData(2)]
        [InlineData(10)]
        public void Leader_AppendCommand_several_times_should_distribute_commands_between_nodes(int nodeCount)
        {
            const int CommandCount = 5;
            var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount * 2)
                .All()
                .With(x => x.Completion = null)
                .Build()
                .ToList();

            var leader = CreateNetworkAndGetLeader(nodeCount, messageTimeout: 10000);
            var entriesAppended = new Dictionary<string, List<LogEntry>>();
            Nodes.ToList().ForEach(node =>
            {
                entriesAppended.Add(node.Name, new List<LogEntry>());
                node.EntriesAppended += logEntries => entriesAppended[node.Name].AddRange(logEntries);
            });

            var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
            var commitsAppliedEvent = new ManualResetEventSlim();
            nonLeaderNode.CommitApplied += (cmd) =>
            {
                if (cmd.AssignedIndex == commands.Last().AssignedIndex)
                    commitsAppliedEvent.Set();
            };

            commands.Take(CommandCount).ToList().ForEach(leader.AppendCommand);
            commands.Skip(CommandCount).ToList().ForEach(leader.AppendCommand);

            var millisecondsTimeout = 10000 * nodeCount;
            Assert.True(commitsAppliedEvent.Wait(millisecondsTimeout), "within " + millisecondsTimeout + " sec. non leader node should have all relevant commands committed");

            var committedCommands = nonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
                                                                                    .OfType<DictionaryCommand.Set>().ToList();

            Assert.Equal(10, committedCommands.Count);
            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(commands[i].Value, committedCommands[i].Value);
                Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
            }
        }
    }
}
