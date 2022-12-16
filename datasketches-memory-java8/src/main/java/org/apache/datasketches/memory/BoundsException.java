package org.apache.datasketches.memory;

/**
 * Specifically used for detecting index bounds violations.
 *
 * @author Lee Rhodes
 */
public class BoundsException extends MemoryException {
  static final long serialVersionUID = 9001L; //prime

  public BoundsException() {
    super();
  }

  public BoundsException(String message) {
    super(message);
  }

}
