package net.ravendb.abstractions.data;

import net.ravendb.abstractions.basic.UseSharpEnum;

@UseSharpEnum
public enum UuidType {

  DOCUMENTS((byte)1),
  ATTACHMENTS((byte)2),
  DOCUMENTTRANSACTIONS((byte)3),
  MAPPEDRESULTS((byte)4),
  REDUCERESULTS((byte)5),
  SCHEDULEDREDUCTIONS((byte)6),
  QUEUE((byte)7),
  TASKS((byte)8),
  INDEXING((byte)9),
  ETAGSYNCHRONIZATION((byte)10);

  private byte value;

  private UuidType(byte value) {
    this.value = value;
  }

  /**
   * @return the value
   */
  public byte getValue() {
    return value;
  }


}
