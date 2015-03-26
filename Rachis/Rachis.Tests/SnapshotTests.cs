using System.Diagnostics;
using System.Linq;
using System.Threading;
using FizzWare.NBuilder;
using FluentAssertions;
using FluentAssertions.Events;
using Rachis.Commands;
using Rachis.Storage;
using Rachis.Transport;
using Voron;
using Xunit;
using Xunit.Extensions;

namespace Rachis.Tests
{
	public class SnapshotTests : RaftTestsBase
	{

		[Fact]
		public void CanProperlySnapshot()
		{
			using (var state = new PersistentState("self", StorageEnvironmentOptions.CreateMemoryOnly(), CancellationToken.None)
			{
				CommandSerializer = new JsonCommandSerializer()
			})
			{
				state.UpdateTermTo(null, 1);
				state.AppendToLeaderLog(new NopCommand());
				for (int i = 0; i < 5; i++)
				{
					state.AppendToLeaderLog(new DictionaryCommand.Set
					{
						Key = i.ToString(),
						Value = i
					});
				}

				state.MarkSnapshotFor(6, 1, 5);

				state.AppendToLeaderLog(new DictionaryCommand.Set
				{
					Key = "1",
					Value = 4
				});

				var lastLogEntry = state.LastLogEntry();

				Assert.Equal(7, lastLogEntry.Index);
			}
		}

		[Theory]
		[InlineData(1)]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(5)]
		[InlineData(7)]
		public void AfterSnapshotInstalled_CanContinueGettingLogEntriesNormally(int amount)
		{
			var leader = CreateNetworkAndGetLeader(amount);
			leader.Options.MaxLogLengthBeforeCompaction = 5;
			var snapshot = WaitForSnapshot(leader);
			var commits = WaitForCommitsOnCluster(
				machine => machine.Data.Count == 5);
			for (int i = 0; i < 5; i++)
			{
				leader.AppendCommand(new DictionaryCommand.Set
				{
					Key = i.ToString(),
					Value = i
				});
			}
			Assert.True(snapshot.Wait(3000));
			Assert.True(commits.Wait(3000));

			Assert.NotNull(leader.StateMachine.GetSnapshotWriter());

			var newNode = NewNodeFor(leader);
			WriteLine("<-- adding node");
			var waitForSnapshotInstallation = WaitForSnapshotInstallation(newNode);

            Assert.True(leader.AddToClusterAsync(new NodeConnectionInfo { Name = newNode.Name }).Wait(3000));

            Assert.True(waitForSnapshotInstallation.Wait(3000));

			Assert.Equal(newNode.CurrentLeader, leader.Name);

			var commit = WaitForCommit(newNode,
				machine => machine.Data.ContainsKey("c"));

			leader.AppendCommand(new DictionaryCommand.Set
			{
				Key = "c",
				Value = 1
			});

            Assert.True(commit.Wait(3000));

			var dictionary = ((DictionaryStateMachine)newNode.StateMachine).Data;
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal(i, dictionary[i.ToString()]);
			}
			Assert.Equal(1, dictionary["c"]);
		}

		[Fact]
		public void Snapshot_after_enough_command_applies_snapshot_is_applied_only_once()
		{
			var snapshotCreationEndedEvent = new ManualResetEventSlim();
			const int commandsCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandsCount)
				.All()
				.With(x => x.Completion = null)
				.Build()
				.ToList();
			var appliedAllCommandsEvent = new CountdownEvent(commandsCount);

			var leader = CreateNetworkAndGetLeader(3);

			leader.MonitorEvents();
			leader.CreatedSnapshot += snapshotCreationEndedEvent.Set;
			leader.CommitIndexChanged += (old, @new) => appliedAllCommandsEvent.Signal();

			leader.Options.MaxLogLengthBeforeCompaction = commandsCount - 3;
			commands.ForEach(leader.AppendCommand);

			Assert.True(appliedAllCommandsEvent.Wait(3000));
			Assert.True(snapshotCreationEndedEvent.Wait(3000));

			//should only raise the event once
			leader.ShouldRaise("CreatedSnapshot");
			leader.GetRecorderForEvent("CreatedSnapshot")
				  .Should().HaveCount(1);
		}


		[Fact]
		public void Snaphot_after_enough_command_applies_snapshot_is_created()
		{
			var snapshotCreationEndedEvent = new ManualResetEventSlim();
			const int commandsCount = 9;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(commandsCount)
						.All()
						.With(x => x.Completion = null)
						.Build()
						.ToList();

			var leader = CreateNetworkAndGetLeader(3);
			var lastLogEntry = leader.PersistentState.LastLogEntry();
			leader.Options.MaxLogLengthBeforeCompaction = commandsCount - 4;

			var appliedAllCommandsEvent = new CountdownEvent(commandsCount);
			leader.CreatedSnapshot += snapshotCreationEndedEvent.Set;

			leader.CommitApplied += cmd =>
			{
				if (cmd is DictionaryCommand.Set)
				{
					appliedAllCommandsEvent.Signal();
				}
			};

			WriteLine("<--- Started appending commands..");
			commands.ForEach(leader.AppendCommand);
			WriteLine("<--- Ended appending commands..");

			var millisecondsTimeout = Debugger.IsAttached ? 600000 : 4000;
			Assert.True(snapshotCreationEndedEvent.Wait(millisecondsTimeout));
			Assert.True(appliedAllCommandsEvent.Wait(millisecondsTimeout), "Not all commands were applied, there are still " + appliedAllCommandsEvent.CurrentCount + " commands left");

			var entriesAfterSnapshotCreation = leader.PersistentState.LogEntriesAfter(0).ToList();
			Assert.Empty(entriesAfterSnapshotCreation.Where(x=>x.Index == lastLogEntry.Index));
		}
	}
}
