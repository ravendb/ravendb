using System.Collections.Concurrent;
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

    public static void AddFullThreadName(long id, string fullName)
    {
        FullThreadNames[id] = fullName;
    }

    public static void RemoveFullThreadName(long id)
    {
        FullThreadNames.TryRemove(id, out var _);
    }

    public static ThreadInfo ForElector(string threadName, string source)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.Elector(source)
        };
    }

    public static ThreadInfo ForEtlProcess(string threadName, string tag, string name)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.EtlProcess(tag, name)
        };
    }

    public static ThreadInfo ForIndex(string threadName, string idxName, string dbName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.Index(idxName, dbName)
        };
    }

    public static ThreadInfo ForUploadBackupFile(string threadName, string dbName, string targetName, string taskName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.UploadBackupFile(dbName, targetName, taskName)
        };
    }

    public static ThreadInfo ForDeleteBackupFile(string threadName, string dbName, string targetName, string taskName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.DeleteBackupFile(dbName, targetName, taskName)
        };
    }

    public static ThreadInfo ForBackupTask(string threadName, string dbName, string taskName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.BackupTask(dbName, taskName)
        };
    }

    public static ThreadInfo ForIncomingReplication(string threadName, string dbName, string sourceDbName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.IncomingReplication(dbName, sourceDbName)
        };
    }

    public static ThreadInfo ForOutgoingReplication(string threadName, string databaseName, string destination, bool pullReplicationAsHub = false)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.OutgoingReplication(databaseName, destination, pullReplicationAsHub)
        };
    }

    public static ThreadInfo ForTransactionMerging(string threadName, string name)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.TransactionMerging(name)
        };
    }

    public static ThreadInfo ForCandidate(string threadName, string engineTag)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.Candidate(engineTag)
        };
    }

    public static ThreadInfo ForCandidateAmbassador(string threadName, string engineTag, string tag)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.CandidateAmbassador(engineTag, tag)
        };
    }

    public static ThreadInfo ForFollower(string threadName, string connection, long term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.Follower(connection, term)
        };
    }

    public static ThreadInfo ForFollowerAmbassador(string threadName, string tag, string term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.FollowerAmbassador(tag, term)
        };
    }

    public static ThreadInfo ForConsensusLeader(string threadName, string engineTag, long term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.ConsensusLeader(engineTag,  term)
        };
    }

    public static ThreadInfo ForHeartbeatsSupervisor(string threadName, string serverNodeTag, string clusterTag, long term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.HeartbeatsSupervisor(serverNodeTag, clusterTag, term)
        };
    }

    public static ThreadInfo ForHeartbeatsWorker(string threadName, string leader, long term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.HeartbeatsWorker(leader, term)
        };
    }

    public static ThreadInfo ForClusterObserver(string threadName, long term)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.ClusterObserver(term)
        };
    }

    public static ThreadInfo ForClusterMaintenanceSetupTask(string threadName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.ClusterMaintenanceSetupTask()
        };
    }

    public static ThreadInfo ForUpdateTopologyChangeNotificationTask(string threadName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.UpdateTopologyChangeNotificationTask()
        };
    }

    public static ThreadInfo ForBackup(string threadName, string backupName, string databaseName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.Backup(backupName, databaseName)
        };
    }
    public static ThreadInfo ForCpuCreditsMonitoring(string threadName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.CpuCreditsMonitoring()
        };
    }

    public static ThreadInfo ForPullReplicationAsSink(string threadName, string destinationDatabase, string destinationUrl)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.PullReplicationAsSink(destinationDatabase, destinationUrl)
        };
    }

    public static ThreadInfo ForQueueSinkProcess(string threadName, string tag, string name)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.QueueSinkProcess(tag, name)
        };
    }

    public static ThreadInfo ForClusterTransactions(string threadName, string databaseName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.ClusterTransaction(databaseName)
        };
    }

    public static ThreadInfo ForUploadRetiredAttachment(string threadName, string dbName, string targetName, string taskName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.UploadRetiredAttachment(dbName, targetName)
        };
    }

    public static ThreadInfo ForDeleteRetiredAttachment(string threadName, string dbName, string targetName, string taskName)
    {
        return new ThreadInfo(threadName)
        {
            Details = new ThreadDetails.DeleteRetiredAttachment(dbName, targetName)
        };
    }

    public sealed class ThreadDetails
    {
        public interface IThreadDetails
        {
            public string GetShortName();
        }

        public sealed class Elector : IThreadDetails
        {
            private readonly string _source;

            public Elector(string source)
            {
                _source = source;
            }

            public string GetShortName()
            {
                return $"Elctr {_source}";
            }
        }

        public sealed class EtlProcess : IThreadDetails
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
                return $"Etl {_tag} {_name}";
            }
        }

        public class QueueSinkProcess : IThreadDetails
        {
            private readonly string _tag;
            private readonly string _name;

            public QueueSinkProcess(string tag, string name)
            {
                _tag = tag;
                _name = name;
            }

            public string GetShortName()
            {
                return $"QuSnk {_tag} {_name}";
            }
        }

        public sealed class Index : IThreadDetails
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
                return $"Idx {_dbName} {_idxName}";
            }
        }

        public sealed class UploadBackupFile : IThreadDetails
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
                return $"UpBkp {_dbName} to {_targetName}";
            }
        }

        public sealed class DeleteBackupFile : IThreadDetails
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
                return $"DlBkp {_dbName} from {_targetName}";
            }
        }

        public sealed class BackupTask : IThreadDetails
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
                return $"BkpTsk {_dbName}";
            }
        }

        public sealed class IncomingReplication : IThreadDetails
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
                return $"IncRep {_dbName} from {_sourceDbName}";
            }
        }

        public sealed class OutgoingReplication : IThreadDetails
        {
            private readonly string _databaseName;
            private readonly string _destination;
            private readonly bool _pullReplicationAsHub;

            public OutgoingReplication(string databaseName, string destination, bool pullReplicationAsHub)
            {
                _databaseName = databaseName;
                _destination = destination;
                _pullReplicationAsHub = pullReplicationAsHub;
            }

            public string GetShortName()
            {
                if (_pullReplicationAsHub)
                {
                    return $"PllRepHb {_databaseName} to {_destination}";
                }

                return $"OutRpl {_databaseName} to {_destination}";
            }
        }

        public sealed class TransactionMerging : IThreadDetails
        {
            private readonly string _name;

            public TransactionMerging(string name)
            {
                _name = name;
            }

            public string GetShortName()
            {
                return $"TxMrgr {_name}";
            }
        }

        public sealed class Candidate : IThreadDetails
        {
            private readonly string _engineTag;

            public Candidate(string engineTag)
            {
                _engineTag = engineTag;
            }

            public string GetShortName()
            {
                return $"Cnddte for {_engineTag}";
            }
        }

        public sealed class CandidateAmbassador : IThreadDetails
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

        public sealed class Follower : IThreadDetails
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
                return $"Fllwr {_connection}";
            }
        }

        public sealed class FollowerAmbassador : IThreadDetails
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
                return $"FllwrAmb {_tag}";
            }
        }

        public sealed class ConsensusLeader : IThreadDetails
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
                return $"CnsnsLdr-{_engineTag} IT {_term}";
            }
        }

        public sealed class HeartbeatsSupervisor : IThreadDetails
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
                return $"HrtbtSpv {_serverNodeTag} to {_clusterTag}";
            }
        }

        public sealed class HeartbeatsWorker : IThreadDetails
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
                return $"HrtbtWrkr {_leader} IT {_term}";
            }
        }

        public sealed class ClusterObserver : IThreadDetails
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

        public sealed class ClusterMaintenanceSetupTask : IThreadDetails
        {
            public ClusterMaintenanceSetupTask()
            {
            }

            public string GetShortName()
            {
                return "Cluster Maintenance Setup Task";
            }
        }

        public sealed class UpdateTopologyChangeNotificationTask : IThreadDetails
        {
            public UpdateTopologyChangeNotificationTask()
            {
            }

            public string GetShortName()
            {
                return "Update Topology Change Notification Task";
            }
        }

        public sealed class Backup : IThreadDetails
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
                return $"Bkp {_databaseName}";
            }
        }

        public sealed class CpuCreditsMonitoring : IThreadDetails
        {
            public CpuCreditsMonitoring()
            {
            }

            public string GetShortName()
            {
                return "CPU Credits Monitoring";
            }
        }

        public sealed class PullReplicationAsSink : IThreadDetails
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
                return $"PllRepSnk {_destinationDatabase} at {_destinationUrl}";
            }
        }

        public class ClusterTransaction : IThreadDetails
        {
            private readonly string _db;
            public ClusterTransaction(string dbName)
            {
                _db = dbName;
            }

            public string GetShortName()
            {
                return $"ClstrTx {_db}";
            }
        }

        public sealed class UploadRetiredAttachment : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _targetName;

            public UploadRetiredAttachment(string dbName, string targetName)
            {
                _dbName = dbName;
                _targetName = targetName;
            }

            public string GetShortName()
            {
                return $"UpRetAtt {_dbName} to {_targetName}";
            }
        }

        public sealed class DeleteRetiredAttachment : IThreadDetails
        {
            private readonly string _dbName;
            private readonly string _targetName;

            public DeleteRetiredAttachment(string dbName, string targetName)
            {
                _dbName = dbName;
                _targetName = targetName;
            }

            public string GetShortName()
            {
                return $"DelRetAtt {_dbName} to {_targetName}";
            }
        }

    }

    public sealed class ThreadInfo
    {
        public ThreadInfo(string threadName)
        {
            FullName = threadName;
        }

        public string FullName { get; set; }

        public ThreadDetails.IThreadDetails Details { get; set; }
    }
}
