package mvsync;

import org.junit.Test;

import static org.junit.Assert.*;

public class RecordColumnInfoTest {
  @Test
  public void basicTest() {

    RecordColumnInfo rc1 = new RecordColumnInfo("k1", "value1");
    RecordColumnInfo rc2 = new RecordColumnInfo("k2", "value2");
    RecordColumnInfo rc3Null = new RecordColumnInfo("k1", null);
    RecordColumnInfo rc4Null = new RecordColumnInfo("k1", null);

    assertTrue(rc1.equals(rc1));
    assertFalse(rc1.equals(rc2));
    assertTrue(rc3Null.equals(rc3Null));
    assertTrue(rc3Null.equals(rc4Null));
    assertFalse(rc1.equals(rc3Null));

    assertEquals("k1:value1", rc1.toString());
    assertEquals("k2:value2", rc2.toString());
    assertEquals("k1", rc3Null.toString());
    assertEquals("k1", rc4Null.toString());
  }
}
