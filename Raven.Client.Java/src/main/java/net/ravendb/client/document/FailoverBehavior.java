package net.ravendb.client.document;

import net.ravendb.abstractions.basic.SerializeUsingValue;
import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
@SerializeUsingValue
public enum FailoverBehavior {

  /** Allow to read from the secondary server(s), but immediately fail writes
   * to the secondary server(s).
   *
   * This is usually the safest approach, because it means that you can still serve
   * read requests when the primary node is down, but don't have to deal with replication
   * conflicts if there are writes to the secondary when the primary node is down.
   */
  ALLOW_READS_FROM_SECONDARIES(1),

  /** Allow to read from the secondary server(s), and allow writes to the secondary server(s).
   * Choosing this option requires that you'll have some way of propagating changes
   * made to the secondary server(s) to the primary node when the primary goes back
   * up.
   * A typical strategy to handle this is to make sure that the replication is setup
   * in a master/master relationship, so any writes to the secondary server will be
   * replicated to the master server.
   * Please note, however, that this means that your code must be prepared to handle
   * conflicts in case of different writes to the same document across nodes.
   */

  ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES(3),

  /** Immediately fail the request, without attempting any failover. This is true for both
   * reads and writes. The RavenDB client will not even check that you are using replication.
   *
   * This is mostly useful when your replication setup is meant to be used for backups / external
   * needs, and is not meant to be a failover storage.
   */
  FAIL_IMMEDIATELY(0),

  /** Read requests will be spread across all the servers, instead of doing all the work against the master.
   * Write requests will always go to the master.
   *
   * This is useful for striping, spreading the read load among multiple servers. The idea is that this will give us better read performance overall.
   * A single session will always use the same server, we don't do read striping within a single session.
   * Note that using this means that you cannot set UserOptimisticConcurrency to true, because that would generate concurrency exceptions.
   * If you want to use that, you have to open the session with ForceReadFromMaster set to true.
   */
  READ_FROM_ALL_SERVERS(1024);

  private int value;

  /**
   * @return the value
   */
  public int getValue() {
    return value;
  }

  private FailoverBehavior(int value) {
    this.value = value;
  }

}
