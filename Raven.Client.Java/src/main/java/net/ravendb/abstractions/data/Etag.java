package net.ravendb.abstractions.data;

import java.nio.ByteBuffer;
import java.util.UUID;

import net.ravendb.abstractions.basic.Reference;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;


public class Etag implements Comparable<Etag> {

  private long restarts;
  private long changes;

  /**
   * @return the restarts
   */
  public long getRestarts() {
    return restarts;
  }


  @Override
  public Etag clone() throws CloneNotSupportedException {
    return new Etag(restarts, changes);
  }

  /**
   * @return the changes
   */
  public long getChanges() {
    return changes;
  }

  public Etag() {
    //empty by design
  }

  public Etag(String str) {
    Etag parse = parse(str);
    this.restarts = parse.restarts;
    this.changes = parse.changes;
  }

  public Etag(UuidType type, long restarts, long changes) {
    this.restarts = ((long) type.getValue() << 56) | restarts;
    this.changes = changes;
  }

  public byte[] toByteArray() {
    byte[] block1 = longToBytes(Long.valueOf(restarts));
    byte[] block2 = longToBytes(Long.valueOf(changes));
    return ArrayUtils.addAll(block1, block2);
  }

  private byte[] longToBytes(long x) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(x);
    return buffer.array();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (byte b: toByteArray()) {
      sb.append(String.format("%02X", b));
    }
    sb.insert(8, "-")
    .insert(13, "-")
    .insert(18, "-")
    .insert(23, "-");
    return sb.toString();
  }

  public static long bytesToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.put(bytes);
    buffer.flip();//need flip
    return buffer.getLong();
  }

  public static Etag parse(byte[] bytes) {
    byte[] arr1 = ArrayUtils.subarray(bytes, 0, 8);
    byte[] arr2 = ArrayUtils.subarray(bytes, 8, 16);
    return new Etag(bytesToLong(arr1), bytesToLong(arr2));
  }

  public static boolean tryParse(String str, Reference<Etag> etag) {
    try {
      Etag value = parse(str);
      etag.value = value;
      return true;
    } catch (Exception e) {
      etag.value = null;
      return false;
    }
  }

  private static byte hexStringToByte(String s) {
    return (byte) ((Character.digit(s.charAt(0), 16) << 4)
        + Character.digit(s.charAt(0+1), 16));
  }

  public static Etag parse(String str) {

    if (StringUtils.isEmpty(str)) {
      throw new IllegalArgumentException("Str cannot be empty or null");
    }
    if (str.length() != 36) {
      throw new IllegalArgumentException("Str must be 36 characters");
    }

    byte[] bytes = new byte[] {
        hexStringToByte(str.substring(0,2)),
        hexStringToByte(str.substring(2,4)),
        hexStringToByte(str.substring(4,6)),
        hexStringToByte(str.substring(6,8)),
        hexStringToByte(str.substring(9,11)),
        hexStringToByte(str.substring(11,13)),
        hexStringToByte(str.substring(14,16)),
        hexStringToByte(str.substring(16,18)),
        hexStringToByte(str.substring(19,21)),
        hexStringToByte(str.substring(21,24)),
        hexStringToByte(str.substring(24,26)),
        hexStringToByte(str.substring(26,28)),
        hexStringToByte(str.substring(28,30)),
        hexStringToByte(str.substring(30,32)),
        hexStringToByte(str.substring(32,34)),
        hexStringToByte(str.substring(34,36))
    };

    byte[] arr1 = ArrayUtils.subarray(bytes, 0, 8);
    byte[] arr2 = ArrayUtils.subarray(bytes, 8, 16);
    return new Etag(bytesToLong(arr1), bytesToLong(arr2));
  }

  public static Etag invalidEtag() {
    return new Etag(-1, -1);
  }

  public static Etag empty() {
    return new Etag(0,0);
  }

  private Etag(long restarts, long changes) {
    super();
    this.restarts = restarts;
    this.changes = changes;
  }

  public Etag setup(UuidType type, long restartsNum) {
    return new Etag((((long) type.getValue()) << 56) | restartsNum, changes);
  }

  public Etag incrementBy(int amount) {
    return new Etag(restarts, changes + amount);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (changes ^ (changes >>> 32));
    result = prime * result + (int) (restarts ^ (restarts >>> 32));
    return result;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Etag other = (Etag) obj;
    if (changes != other.changes)
      return false;
    if (restarts != other.restarts)
      return false;
    return true;
  }


  public static Etag random() {
    return new Etag(UUID.randomUUID().toString());
  }


  @Override
  public int compareTo(Etag other) {
    if (other == null) {
      return -1;
    }
    long sub = restarts - other.restarts;
    if (Math.abs(sub) > 0) {
      return sub > 0 ? 1 : -1;
    }
    sub = changes - other.changes;
    if (sub != 0) {
      return sub > 0 ? 1 : -1;
    }
    return 0;
  }




}
