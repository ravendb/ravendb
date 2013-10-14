package net.ravendb.client.connection.profiling;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Delegates;


/**
 *  Information about a particular session
 */
public class ProfilingInformation {

  /**
   *  A hook that allows extensions to provide additional information about the created context
   */
  public static Action1<ProfilingInformation> onContextCreated = Delegates.delegate1();

  /**
   * Create a new instance of profiling information and provide additional context information
   * @param currentSessionId
   * @return
   */
  public static ProfilingInformation createProfilingInformation(UUID currentSessionId) {
    ProfilingInformation profilingInformation = new ProfilingInformation(currentSessionId);
    onContextCreated.apply(profilingInformation);
    return profilingInformation;
  }

  private Map<String, String> context;
  private List<RequestResultArgs> requests = new ArrayList<>();
  private UUID id;
  private Date at = new Date();
  private double durationMilleseconds;

  public Map<String, String> getContext() {
    return context;
  }
  public void setContext(Map<String, String> context) {
    this.context = context;
  }
  public List<RequestResultArgs> getRequests() {
    return requests;
  }
  public void setRequests(List<RequestResultArgs> requests) {
    this.requests = requests;
  }
  public UUID getId() {
    return id;
  }
  public void setId(UUID id) {
    this.id = id;
  }
  public Date getAt() {
    return at;
  }
  public void setAt(Date at) {
    this.at = at;
  }
  public double getDurationMilleseconds() {
    return durationMilleseconds;
  }
  public void setDurationMilleseconds(double durationMilleseconds) {
    this.durationMilleseconds = durationMilleseconds;
  }

  private ProfilingInformation(UUID sessionId) {
    id = sessionId != null ? sessionId : UUID.randomUUID();
    context = new HashMap<>();
  }




}
