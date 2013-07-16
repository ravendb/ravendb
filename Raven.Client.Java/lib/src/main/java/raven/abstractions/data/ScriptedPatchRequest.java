package raven.abstractions.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.json.linq.RavenJObject;

/**
 * A Advanced patch request for a specified document (using JavaScript)
 */
public class ScriptedPatchRequest {

  private String script;
  private Map<String, Object> values;

  public ScriptedPatchRequest() {
    values = new HashMap<String, Object>();
  }
  public String getScript() {
    return script;
  }
  public Map<String, Object> getValues() {
    return values;
  }
  public void setScript(String script) {
    this.script = script;
  }
  public void setValues(Map<String, Object> values) {
    this.values = values;
  }

  public static ScriptedPatchRequest fromJson(RavenJObject patchRequestJson) {
    try {
      return JsonExtensions.getDefaultObjectMapper().readValue(patchRequestJson.toString(), ScriptedPatchRequest.class);
    } catch (IOException e ){
      throw new RuntimeException("Unable to parse ScriptedPatchRequest", e);
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(script).append(values).hashCode();
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ScriptedPatchRequest other = (ScriptedPatchRequest) obj;
    return new EqualsBuilder().append(script, other.script).append(values, other.values).isEquals();
  }

}
