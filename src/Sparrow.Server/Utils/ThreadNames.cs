using System;
using System.Collections.Concurrent;
using System.Formats.Asn1;
using Sparrow.Platform;

namespace Sparrow.Server.Utils;

public static class ThreadNames
{
    public static readonly ConcurrentDictionary<long, string> FullThreadNames = new();

    public static string GetNameToUse(ThreadInfo threadInfo)
    {
        if (PlatformDetails.RunningOnPosix == false)
            return threadInfo.FullName;

        return threadInfo.Details.GetShortName();
    }

    public static string GetShortName(ThreadDetails.IThreadDetails threadDetails)
    {
        return threadDetails.GetShortName();
        
    }

    public static void AddFullThreadName(long id, string fullName)
    {
        FullThreadNames[id] = fullName;
    }

    public static void RemoveFullThreadName(long id)
    {
        FullThreadNames.TryRemove(id, out var _);
    }
    
    public class ThreadDetails
    {
        public interface IThreadDetails
        {
            public string GetShortName();
        }

        public class Elector : IThreadDetails
        {
            private readonly string _source;

            public Elector(string source)
            {
                _source = source;
            }

            public string GetShortName()
            {
                return $"Elector {_source}";
            }
        }

        public class EtlProcess : IThreadDetails
        {
            private readonly string _tag;
            private readonly string _name;

            public EtlProcess(string tag, string name)
            {
                _tag = tag;
                _name = name;
            }

            public string GetShortName()
            {
                return $"{_tag} {_name}";
            }
        }

        public class Index : IThreadDetails
        {
            private readonly string _idxName;
            private readonly string _dbName;

            public Index(string idxName, string dbName)
            {
                _idxName = idxName;
                _dbName = dbName;
            }

            public string GetShortName()
            {
                return $"IX:{_dbName} {_idxName}";
            }
        }

        public class UploadBackupFile : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _targetName;
            private readonly string _taskName;

            public UploadBackupFile(string dbName, string targetName, string taskName)
            {
                _dbName = dbName;
                _targetName = targetName;
                _taskName = taskName;
            }

            public string GetShortName()
            {
                return $"UBF {_dbName} to {_targetName}";
            }
        }

        public class DeleteBackupFile : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _targetName;
            private readonly string _taskName;

            public DeleteBackupFile(string dbName, string targetName, string taskName)
            {
                _dbName = dbName;
                _targetName = targetName;
                _taskName = taskName;
            }

            public string GetShortName()
            {
                return $"DBF {_dbName} from {_targetName}";
            }
        }

        public class BackupTask : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _taskName;

            public BackupTask(string dbName, string taskName)
            {
                _dbName = dbName;
                _taskName = taskName;
            }

            public string GetShortName()
            {
                return $"Backup {_dbName}";
            }
        }

        public class IncomingReplication : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _sourceDbName;

            public IncomingReplication(string dbName, string sourceDbName)
            {
                _dbName = dbName;
                _sourceDbName = sourceDbName;
            }

            public string GetShortName()
            {
                return $"RepInc {_dbName} from {_sourceDbName}";
            }
        }

        public class OutgoingReplication : IThreadDetails
        {
            private readonly string _databaseName;
            private readonly string _destination;
            private readonly bool _pullReplicationAsHub;

            public OutgoingReplication(string databaseName, string destination, bool pullReplicationAsHub = false)
            {
                _databaseName = databaseName;
                _destination = destination;
                _pullReplicationAsHub = pullReplicationAsHub;
            }

            public string GetShortName()
            {
                if (_pullReplicationAsHub)
                {
                    return $"RepPAH {_databaseName} to {_destination}";
                }

                return $"RepO {_databaseName} to {_destination}";
            }
        }

        public class TransactionMerging : IThreadDetails
        {
            private readonly string _name;

            public TransactionMerging(string name)
            {
                _name = name;
            }

            public string GetShortName()
            {
                return $"TXMRG {_name}";
            }
        }

        public class Candidate : IThreadDetails
        {
            private readonly string _engineTag;

            public Candidate(string engineTag)
            {
                _engineTag = engineTag;
            }

            public string GetShortName()
            {
                return $"Candidate for {_engineTag}";
            }
        }

        public class CandidateAmbassador : IThreadDetails
        {
            private readonly string _engineTag;
            private readonly string _tag;

            public CandidateAmbassador(string engineTag, string tag)
            {
                _engineTag = engineTag;
                _tag = tag;
            }

            public string GetShortName()
            {
                return $"CndAm for {_engineTag}>{_tag}";
            }
        }

        public class Follower : IThreadDetails
        {
            private readonly string _connection;
            private readonly long _term;

            public Follower(string connection, long term)
            {
                _connection = connection;
                _term = term;
            }

            public string GetShortName()
            {
                return $"Follower {_connection}";
            }
        }

        public class FollowerAmbassador : IThreadDetails
        {
            private readonly string _tag;
            private readonly string _term;

            public FollowerAmbassador(string tag, string term)
            {
                _tag = tag;
                _term = term;
            }

            public string GetShortName()
            {
                return $"FollowAmb {_tag}";
            }
        }

        public class ConsensusLeader : IThreadDetails
        {
            private readonly string _engineTag;
            private readonly long _term;

            public ConsensusLeader(string engineTag, long term)
            {
                _engineTag = engineTag;
                _term = term;
            }

            public string GetShortName()
            {
                return $"ConsensusL-{_engineTag} IT {_term}";
            }
        }

        public class HeartbeatsSupervisor : IThreadDetails
        {
            private readonly string _serverNodeTag;
            private readonly string _clusterTag;
            private readonly long _term;

            public HeartbeatsSupervisor(string serverNodeTag, string clusterTag, long term)
            {
                _serverNodeTag = serverNodeTag;
                _clusterTag = clusterTag;
                _term = term;
            }

            public string GetShortName()
            {
                return $"HBeatS {_serverNodeTag} to {_clusterTag}";
            }
        }

        public class HeartbeatsWorker : IThreadDetails
        {
            private readonly string _leader;
            private readonly long _term;

            public HeartbeatsWorker(string leader, long term)
            {
                _leader = leader;
                _term = term;
            }

            public string GetShortName()
            {
                return $"HBbeatW {_leader} IT {_term}";
            }
        }

        public class ClusterObserver : IThreadDetails
        {
            private readonly long _term;

            public ClusterObserver(long term)
            {
                _term = term;
            }

            public string GetShortName()
            {
                return $"Observer {_term}";
            }
        }

        public class ClusterMaintenanceSetupTask : IThreadDetails
        {
            public ClusterMaintenanceSetupTask()
            {
            }

            public string GetShortName()
            {
                return "Cluster Maintenance Setup Task";
            }
        }

        public class UpdateTopologyChangeNotificationTask : IThreadDetails
        {
            public UpdateTopologyChangeNotificationTask()
            {
            }

            public string GetShortName()
            {
                return "Update Topology Change Notification Task";
            }
        }

        public class Backup : IThreadDetails
        {
            private readonly string _backupName;
            private readonly string _databaseName;

            public Backup(string backupName, string databaseName)
            {
                _backupName = backupName;
                _databaseName = databaseName;
            }

            public string GetShortName()
            {
                return $"Backup {_databaseName}";
            }
        }

        public class CpuCreditsMonitoring : IThreadDetails
        {
            public CpuCreditsMonitoring()
            {
            }

            public string GetShortName()
            {
                return "CPU Credits Monitoring";
            }
        }
        public class PullReplicationAsSink : IThreadDetails
        {
            private readonly string _destinationDatabase;
            private readonly string _destinationUrl;
            public PullReplicationAsSink(string destinationDatabase, string destinationUrl)
            {
                _destinationDatabase = destinationDatabase;
                _destinationUrl = destinationUrl;
            }

            public string GetShortName()
            {
                return $"RepPS {_destinationDatabase} at {_destinationUrl}";
            }
        }
    }

    public class ThreadInfo
    {
        public string FullName { get; set; }

        public ThreadDetails.IThreadDetails Details { get; set; }
    }
}
