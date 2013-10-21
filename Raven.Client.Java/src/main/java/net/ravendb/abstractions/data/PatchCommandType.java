package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum PatchCommandType {

  /**
   * Set a property
   */
  SET,
  /**
   * Unset (remove) a property
   */
  UNSET,

  /**
   * Add an item to an array
   */
  ADD,

  /**
   *  Insert an item to an array at a specified position
   */
  INSERT,

  /**
   * Remove an item from an array at a specified position
   */
  REMOVE,

  /**
   *  Modify a property value by providing a nested set of patch operation
   */
  MODIFY,

  /**
   *  Increment a property by a specified value
   */
  INC,

  /**
   * Copy a property value to another property
   */
  COPY,

  /**
   * Rename a property
   */
  RENAME;
}
