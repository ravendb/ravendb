package raven.client.delegates;

import java.lang.reflect.Field;


public interface IdentityPropertyFinder {
  public Boolean find(Field field);
}
