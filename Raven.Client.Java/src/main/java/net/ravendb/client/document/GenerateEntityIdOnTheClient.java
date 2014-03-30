package net.ravendb.client.document;

import java.lang.reflect.Field;
import java.util.UUID;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.converters.ITypeConverter;

import org.apache.commons.lang.reflect.FieldUtils;


public class GenerateEntityIdOnTheClient {
  private final IDocumentStore documentStore;
  private final Function1<Object, String> generateKey;

  public GenerateEntityIdOnTheClient(IDocumentStore documentStore, Function1<Object, String> generateKey) {
    this.documentStore = documentStore;
    this.generateKey = generateKey;
  }

  private Field getIdentityProperty(Class<?> entityType) {
    return documentStore.getConventions().getIdentityProperty(entityType);
  }

  /**
   * Attempts to get the document key from an instance
   * @param entity
   * @param string
   * @return
   */
  public boolean tryGetIdFromInstance(Object entity, Reference<String> idHolder) {
    try {
      Field identityProperty = getIdentityProperty(entity.getClass());
      if (identityProperty != null) {
        Object value = FieldUtils.readField(identityProperty, entity, true);
        if (value instanceof String) {
          idHolder.value = (String) value;
        }
        if (idHolder.value == null && value == null && identityProperty.getType().equals(UUID.class)) {
          // fix for UUID as UUID is nullable type in Java
          value = Constants.EMPTY_UUID;
        }
        if (idHolder.value == null && value != null) { //need convertion
          idHolder.value = documentStore.getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(value, entity.getClass(), true);
          return true;
        }
        return idHolder.value != null;
      }
      idHolder.value = null;
      return false;
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Tries to get the identity.
   * @param entity
   * @return
   */
  public String getOrGenerateDocumentKey(Object entity) {
    Reference<String> idHolder = new Reference<>();
    tryGetIdFromInstance(entity, idHolder);
    String id = idHolder.value;
    if (id == null) {
      // Generate the key up front
      id = generateKey.apply(entity);
    }

    if (id != null && id.startsWith("/")) {
      throw new IllegalStateException("Cannot use value '" + id + "' as a document id because it begins with a '/'");
    }
    return id;
  }

  public String generateDocumentKeyForStorage(Object entity) {
    String id = getOrGenerateDocumentKey(entity);
    trySetIdentity(entity, id);
    return id;
  }

  /**
   * Tries to set the identity property
   */
  public void trySetIdentity(Object entity, String id) {
    Class<?> entityType = entity.getClass();
    Field identityProperty = documentStore.getConventions().getIdentityProperty(entityType);

    if (identityProperty == null) {
      return;
    }

    setPropertyOrField(identityProperty.getType(), entity, identityProperty, id);
  }

  private void setPropertyOrField(Class<?> propertyOrFieldType, Object entity, Field field, String id) {
    try {
      if (String.class.equals(propertyOrFieldType)) {
        FieldUtils.writeField(field, entity, id, true);
      } else { // need converting
        for (ITypeConverter converter : documentStore.getConventions().getIdentityTypeConvertors()) {
          if (converter.canConvertFrom(propertyOrFieldType)) {
            FieldUtils.writeField(field, entity, converter.convertTo(documentStore.getConventions().getFindIdValuePartForValueTypeConversion().find(entity, id)), true);
            return;
          }
        }
        throw new IllegalArgumentException("Could not convert identity to type " + propertyOrFieldType +
            " because there is not matching type converter registered in the conventions' IdentityTypeConvertors");
      }
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }


}
