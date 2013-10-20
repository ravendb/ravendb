package net.ravendb.client;

import java.util.LinkedHashSet;
import java.util.Set;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.document.RavenLoadConfiguration;


public class LoadConfigurationFactory {

  private Set<Action1<ILoadConfiguration>> actions = new LinkedHashSet<>();

  public LoadConfigurationFactory() {
    super();
  }

  protected LoadConfigurationFactory(Set<Action1<ILoadConfiguration>> actions, Action1<ILoadConfiguration> newAction) {
    this.actions.addAll(actions);
    this.actions.add(newAction);
  }

  public LoadConfigurationFactory addQueryParam(final String name, final RavenJToken value) {
    return new LoadConfigurationFactory(actions, new Action1<ILoadConfiguration>() {
      @Override
      public void apply(ILoadConfiguration loadConfiguration) {
        loadConfiguration.addQueryParam(name, value);
      }
    });
  }

  public LoadConfigurationFactory addQueryParam(final String name, final Object value) {
    return new LoadConfigurationFactory(actions, new Action1<ILoadConfiguration>() {
      @Override
      public void apply(ILoadConfiguration loadConfiguration) {
        loadConfiguration.addQueryParam(name, new RavenJValue(value));
      }
    });
  }

  public void configure(RavenLoadConfiguration configuration) {
    for (Action1<ILoadConfiguration> action: actions) {
      action.apply(configuration);
    }
  }

}
